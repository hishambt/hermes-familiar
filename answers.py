"""The reads and the actions a Familiar client asks this machine for.

Split out of adapter.py, which had grown to hold the channel, the pairing, the delivery path and all of this in one
file. What lives here is the ANSWER half: the frame a client sends asking for something, the table that maps it to a
handler, and the handlers themselves - sessions, conversations, jobs, the model list, a fork, a rename, a delete.

They are methods on a mixin the adapter inherits rather than a second object, and that is deliberate: they read the
same `self` - the same token, the same URL, the same cron store, the same connection - so a client's request has one
place it can arrive from and one answer travels back on the same connection it came in on.

Three things this half shares with the adapter's own - the delivery POST, what the machine does when a token is
refused, and the two paths the channel speaks - are imported inside the three functions that need them. At module
level the two files would import each other, and Python only tolerates that in one order; a function-local import
has no order to get wrong.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
import urllib
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


#: What a paired machine answers out of its OWN state. These are the paths the app already reads over an
#: instance's HTTP API, answered from Hermes' own store and in the same shape - because the app parses one
#: shape, and a second one would be a second source of truth. A path that is not here is refused by name.
_API_ROUTES = (
	("GET", r"/api/sessions", "_api_list_sessions"),
	("GET", r"/api/sessions/(?P<session>[^/]+)/messages", "_api_session_messages"),
	("POST", r"/api/sessions/(?P<session>[^/]+)/fork", "_api_fork_session"),
	("GET", r"/api/sessions/(?P<session>[^/]+)", "_api_get_session"),
	("PATCH", r"/api/sessions/(?P<session>[^/]+)", "_api_patch_session"),
	("DELETE", r"/api/sessions/(?P<session>[^/]+)", "_api_delete_session"),
	# Not one of Hermes' paths: its own session list hides the sessions a conversation was compressed into, so
	# the machine answers this one itself, out of the same store.
	("GET", r"/familiar/conversation", "_api_conversation"),
	("GET", r"/api/model/options", "_api_model_options"),
	("GET", r"/api/jobs", "_api_list_jobs"),
	("POST", r"/api/jobs", "_api_create_job"),
	("GET", r"/api/jobs/(?P<job>[^/]+)", "_api_get_job"),
	("PATCH", r"/api/jobs/(?P<job>[^/]+)", "_api_update_job"),
	("DELETE", r"/api/jobs/(?P<job>[^/]+)", "_api_delete_job"),
	("POST", r"/api/jobs/(?P<job>[^/]+)/(?P<action>run|pause|resume)", "_api_job_action"),
)

#: How many matches of a search this machine will READ to count them. Counting a search is reading it: Hermes'
#: store counts a listing without a search term, nothing over it counts with one, and the search itself runs in
#: SQL over whole compression chains - so the only thing that can answer "how many" is the store, one row at a
#: time. Past this ceiling it answers ``None``: the machine would rather say it did not count than report a number
#: it never read. A search narrow enough to fill a page is counted long before getting here.


_SEARCH_COUNT_CEILING = 5000


#: The cron store's own limits, copied from the API server so a job made down the connection is held to exactly
#: what one made over HTTP is held to: one rule, whichever door it came through.
_JOB_ID_RE = re.compile(r"[a-f0-9]{12}")
_JOB_UPDATE_FIELDS = {"name", "schedule", "prompt", "deliver", "skills", "skill", "repeat", "enabled"}
_MAX_JOB_NAME = 200
_MAX_JOB_PROMPT = 5000


#: The fields one session of a conversation is reported with: what the app draws, and nothing else of the row.
CONVERSATION_FIELDS = (
	"id",
	"parent_session_id",
	"end_reason",
	"title",
	"source",
	"model",
	"started_at",
	"last_active",
	"ended_at",
	"message_count",
	"tool_call_count",
	"api_call_count",
	"input_tokens",
	"output_tokens",
	"cache_read_tokens",
	"cache_write_tokens",
	"reasoning_tokens",
	"estimated_cost_usd",
	"actual_cost_usd",
)

#: What a conversation's total adds up, session by session.
#:
#: These are what each session SPENT, and a session's spend is its own: a compaction copies the transcript into
#: the child without re-billing a token of it, so calls and tokens are earned once per session and sum.
CONVERSATION_SUMS = (
	"api_call_count",
	"input_tokens",
	"output_tokens",
	"cache_read_tokens",
	"cache_write_tokens",
	"reasoning_tokens",
)

#: What a conversation's total does NOT add up, because a copy of it in a later session is not one more of it.
#:
#: What a conversation takes from the session holding its transcript rather than adding up: a compaction CARRIES the
#: tool calls into the child, so summing them over a chain counts them once per session.
#:
#: The MESSAGE count is not here any more, and must not come back. It is counted once across the whole chain
#: (`_conversation_visible`) and belongs to the conversation's total alone - a session's entry keeps its OWN count,
#: because a list of sessions draws its rows from those entries. Writing the conversation's number into the newest
#: entry put a whole conversation's messages on one session's row, which the History table read.
CONVERSATION_CARRIED = ("tool_call_count",)

#: How a parent ended when the conversation carried on in its child. TWO things say that, and the app needs both to
#: be one conversation: a compaction carries the transcript onto a fresh id, and a reset is the reader's own ``/new``,
#: which starts the next session of the same conversation in a topic - the machine's answers from before it are kept
#: per topic and cross it, so counting the reset as a new conversation drew one side of a topic and not the other.
#: A branch and a subagent run are conversations of their own; the store says which in the same field. The instance own word for it, and the ONLY
#: link that means the same conversation: the same field carries a branch, a reset (a separate conversation by the
#: instance own account) and a subagent run, and those are conversations of their own.
CONTINUATION_END_REASONS = ("compression", "session_reset")

#: A conversation is not this long. The walk stops rather than believing the store.
CONVERSATION_MAX_LINKS = 200

#: How many conversations one call may ask about - the history page asks about a page of rows at once.
CONVERSATION_MAX_IDS = 50


def _number(value: Any) -> float:
	"""A stored count or cost as a number. Anything unreadable counts as nothing."""
	if isinstance(value, bool) or value is None:
		return 0.0

	if isinstance(value, (int, float)):
		return float(value)

	try:
		return float(value)
	except (TypeError, ValueError):
		return 0.0


def _row_dict(cursor: Any, row: Any) -> Any:
	"""One row as a dict, whether the connection hands back tuples or row objects."""
	if row is None:
		return None

	if isinstance(row, dict):
		return row

	return dict(row) if hasattr(row, "keys") else dict(zip([column[0] for column in cursor.description], row))


def _session_select() -> str:
	"""A session row, and when it was last used.

	`last_active` is NOT a column: the machine derives it from the session's heartbeat, its newest message, and its
	start. Asking for it the machine's own way is what keeps this route's answer the same shape as the reads beside
	it, and a session's own recency is what tells two sessions of one conversation apart.
	"""
	from hermes_state_common import _sql_session_last_active

	return f"SELECT s.*, {_sql_session_last_active('s')} AS _last_active FROM sessions s"


def _with_recency(row: Any) -> Any:
	"""The same row, with the derivation the machine makes of a session's recency in place of the raw column."""
	if isinstance(row, dict):
		row["last_active"] = row.pop("_last_active", None) or row.get("last_active")

	return row


def _one(conn: Any, sql: str, params: tuple) -> Any:
	"""One row, or nothing."""
	cursor = conn.execute(sql, params)

	return _with_recency(_row_dict(cursor, cursor.fetchone()))


def _children(conn: Any, session_id: str) -> List[Dict[str, Any]]:
	"""The sessions that point at this one, whole rows: a conversation's numbers live on each of them."""
	cursor = conn.execute(f"{_session_select()} WHERE s.parent_session_id = ?", (session_id,))

	return [row for row in (_with_recency(_row_dict(cursor, row)) for row in cursor.fetchall()) if row]


def _conversation_total(chain: List[Dict[str, Any]], visible: Optional[int] = None) -> Dict[str, Any]:
	"""What the sessions of one conversation add up to.

	`estimated_cost_usd` stays null when no session in the chain reported one: a total nobody gave is not zero,
	and the app decides what to draw from that.

	`visible` is the conversation's own message count, counted once across the chain by `_conversation_visible`. It is
	passed in rather than read off the newest session's entry, because those entries are a SESSION's numbers and are
	drawn as such: a row in a list of sessions must answer for that session, not for its whole lineage.
	"""
	total: Dict[str, Any] = {field: sum(_number(row.get(field)) for row in chain) for field in CONVERSATION_SUMS}
	reported = [row.get("estimated_cost_usd") for row in chain if row.get("estimated_cost_usd") is not None]
	carrier = chain[-1] if chain else {}

	total.update({field: _number(carrier.get(field)) for field in CONVERSATION_CARRIED})
	total["message_count"] = visible if visible is not None else _number(carrier.get("message_count"))
	total["estimated_cost_usd"] = sum(_number(cost) for cost in reported) if reported else None
	total["sessions"] = len(chain)

	return total


def _visible_counts(conn: Any, session_ids: List[str]) -> Dict[str, int]:
	"""How many messages each of these sessions holds that a reader can see, in ONE query.

	The rule below is the app's own (`saysSomething`). It is asked for a page of sessions at once because the
	sessions list answers up to two hundred of them, and a count per row would be a query per row.
	"""
	if not session_ids:
		return {}

	marks = ", ".join("?" for _ in session_ids)
	cursor = conn.execute(
		f"""SELECT session_id, count(*) AS visible FROM messages
		    WHERE session_id IN ({marks})
		      AND coalesce(display_kind, '') <> 'hidden'
		      AND (trim(coalesce(content, '')) <> ''
		           OR coalesce(trim(tool_calls), '') NOT IN ('', '[]', 'null')
		           OR coalesce(reasoning, '') <> ''
		           OR coalesce(reasoning_content, '') <> '')
		    GROUP BY session_id""",
		tuple(session_ids),
	)
	found: Dict[str, int] = {}

	for row in cursor.fetchall():
		held = _row_dict(cursor, row) or {}
		found[str(held.get("session_id"))] = int(held.get("visible") or 0)

	return found


def _conversation_visible(conn: Any, session_ids: List[str]) -> int:
	"""How many messages a reader can see across a whole conversation, counting each one ONCE.

	The sessions of one conversation overlap. A compaction COPIES the transcript into its child - measured on a real
	store: 0 shared row ids and 30 identical (role, content) pairs - so adding the sessions up counts the same message
	once per session (a five-session topic: 170 rows summed against 129 said). Taking one session alone under counts
	the other way: the copy is not complete, and what a compaction dropped from the parent exists nowhere else.

	What tells a copy from its original is the row's own ``display_identity``, and it survives the copy byte for byte.
	When a row has none - nothing in the store measured did, but a store can be older than the column - its own id
	stands in, which only ever matches itself.
	"""
	if not session_ids:
		return 0

	marks = ", ".join("?" for _ in session_ids)
	cursor = conn.execute(
		f"""SELECT count(*) AS visible FROM (
		        SELECT coalesce(nullif(hex(display_identity), ''), 'id:' || id) AS one
		        FROM messages
		        WHERE session_id IN ({marks})
		          AND coalesce(display_kind, '') <> 'hidden'
		          AND (trim(coalesce(content, '')) <> ''
		               OR coalesce(trim(tool_calls), '') NOT IN ('', '[]', 'null')
		               OR coalesce(reasoning, '') <> ''
		               OR coalesce(reasoning_content, '') <> '')
		        GROUP BY one)""",
		tuple(session_ids),
	)
	held = _row_dict(cursor, cursor.fetchone()) or {}

	return int(held.get("visible") or 0)


def _conversation_chain(conn: Any, session_id: str) -> List[Dict[str, Any]]:
	"""The sessions one conversation is, oldest first - the walk itself, and nothing counted.

	A conversation outlives the session it started in: the instance compresses it onto a fresh id and keeps
	`parent_session_id` for lineage. Its OWN session list hides those rows, so nothing downstream can see or count
	them - the walk has to happen where the store is.

	Split from the counting because a page of sessions walks once per ROW and counts once per CONVERSATION: the walk
	is a handful of row reads, while the counting scans every message the conversation holds.
	"""
	start = _one(conn, f"{_session_select()} WHERE s.id = ?", (session_id,))

	if start is None:
		return []

	seen = {start["id"]}
	chain = [start]

	# Up to the session the conversation started in. Every step is a link whose parent ended by compressing.
	current = start

	for _ in range(CONVERSATION_MAX_LINKS):
		parent_id = current.get("parent_session_id")

		if not parent_id or parent_id in seen:
			break

		parent = _one(conn, f"{_session_select()} WHERE s.id = ?", (parent_id,))

		if parent is None or parent.get("end_reason") not in CONTINUATION_END_REASONS:
			break

		seen.add(parent["id"])
		chain.append(parent)
		current = parent

	# And down through every session it carried on into.
	current = start

	for _ in range(CONVERSATION_MAX_LINKS):
		if current.get("end_reason") not in CONTINUATION_END_REASONS:
			break

		carried = [child for child in _children(conn, current["id"]) if child["id"] not in seen]

		if not carried:
			break

		carried.sort(key=lambda child: _number(child.get("started_at")))

		for child in carried:
			seen.add(child["id"])
			chain.append(child)

		current = carried[-1]

	chain.sort(key=lambda row: _number(row.get("started_at")))

	return chain


def _conversation_body(chain: List[Dict[str, Any]], each: Dict[str, int], visible: Optional[int]) -> Dict[str, Any]:
	"""What a conversation reports, given its chain and the count the caller took for it.

	``visible`` is None when the caller asked for the conversation WITHOUT its total: the total is a scan of every
	message the conversation holds, and a reader that does not draw it should not make the store do that. It is then
	null rather than the newest session's count, which is a SESSION's number and would be read as the conversation's.
	"""
	return {
		# Each entry keeps its OWN numbers, and its own count is the number a reader can SEE in it - the same rule as
		# everywhere else, asked for the whole chain in one query. A list of sessions draws its rows from these.
		"sessions": [
			{
				**{field: row.get(field) for field in CONVERSATION_FIELDS},
				"message_count": each.get(str(row.get("id")), row.get("message_count")),
			}
			for row in chain
		],
		"total": None if visible is None else _conversation_total(chain, visible),
	}


def _conversations_for(db: Any, ids: List[str], totals: bool = True) -> Dict[str, Any]:
	"""Which conversation each asked session is, and what it adds up to, in the store the caller opened.

	Asked for a page at once rather than row by row, and for the same reason `_visible_counts` is: a page of twenty
	sessions here spans thirteen conversations, so counting per ROW spends most of its time recomputing a conversation
	the row above it already had. Measured on a real store: 1.9s before, 0.6s after, for identical answers.

	The count a reader is shown comes from the rows a reader can SEE, counting each message once however many sessions
	carried it. Hermes counts every ROW, and a `session_meta` row - written when a session starts, holding nothing, one
	per session - made every conversation read one higher than its thread could ever draw. It goes in the TOTAL and
	nowhere else: an entry keeps its own session's numbers, because the app draws a row of a list from that entry.

	``totals=False`` answers with the conversations and no totals: the sessions list draws a row's own numbers and never
	the conversation's total, and every row of a page that does not need it was still paying for a scan of every message
	the conversation holds.
	"""
	conn = getattr(db, "_conn", None)

	if conn is None:
		raise RuntimeError("the session store came back without a connection to read")

	resolver = getattr(db, "resolve_session_id", None)
	chains: Dict[str, List[Dict[str, Any]]] = {}

	for session_id in ids:
		resolved = resolver(session_id) if resolver is not None else None
		chains[session_id] = _conversation_chain(conn, resolved or session_id)

	# Every session any of those chains holds, in one query: a chain reaches back through sessions the page itself does
	# not list, and their entries need their own counts too.
	every = sorted({str(row.get("id")) for chain in chains.values() for row in chain})
	each = _visible_counts(conn, every)

	# Keyed on the CHAIN, not on the conversation: two rows sharing a chain are two rows whose counting is the same
	# question, and answering it once cannot change what either of them is told. (Which sessions a chain holds depends
	# on which session was asked for - see the walk - so keying on anything looser than the chain itself would be a
	# guess about the walk's rules, and those rules are the machine's, not this page's.)
	found: Dict[str, Any] = {}
	counted: Dict[Tuple[str, ...], Dict[str, Any]] = {}

	for session_id, chain in chains.items():
		if not chain:
			found[session_id] = {"sessions": [], "total": None}

			continue

		key = tuple(sorted(str(row.get("id")) for row in chain))

		if key not in counted:
			counted[key] = _conversation_body(
				chain,
				each,
				_conversation_visible(conn, [row["id"] for row in chain]) if totals else None,
			)

		found[session_id] = counted[key]

	return found


def _job_name(job_id: str) -> Optional[str]:
	"""The job's own name, when this process can see the cron store.

	Best effort on purpose: a delivery must not fail because a name could not be read, and the id is already in
	the payload for a client that wants to resolve the name itself. A gateway delivers through this plugin, so
	the store is the same one the scheduler just read.
	"""
	try:
		from cron.jobs import get_job

		return (get_job(job_id) or {}).get("name") or None
	except Exception:
		logger.debug("[familiar] could not read the name of job %s", job_id, exc_info=True)
		return None


# -- The shapes a client parses -------------------------------------------------


def _api_error(message: str, code: str) -> Dict[str, Any]:
	"""Hermes' own error envelope, so a refusal reads the same through either door."""
	return {"error": {"message": message, "type": "invalid_request_error", "param": None, "code": code}}


def _session_payload(session: Dict[str, Any], visible: Optional[int] = None) -> Dict[str, Any]:
	"""One session, in the shape the instance's API answers with.

	The fields are the API server's own client-safe list (``api_server.py::_session_response``): a full system
	prompt or model config never crosses a client surface, only whether one is there. Copied deliberately and kept
	identical, because the app parses that shape and a second one would drift from it.

	ONE deviation, and it is the number rather than the shape: ``visible`` replaces ``message_count`` with what
	``_visible_counts`` read from the store. The app draws a session's transcript and puts that count beside it, so a
	row of bookkeeping the store counts - a ``session_meta`` row, which holds nothing and exists once per session -
	left the header saying "53 of 54" over 53 messages. The shape stays the API's; the count is the reader's.
	"""
	safe_keys = (
		"id", "source", "user_id", "model", "title", "started_at", "ended_at", "end_reason",
		"message_count", "tool_call_count", "input_tokens", "output_tokens",
		"cache_read_tokens", "cache_write_tokens", "reasoning_tokens", "estimated_cost_usd",
		"actual_cost_usd", "api_call_count", "parent_session_id", "last_active", "preview",
		"_lineage_root_id", "pinned", "archived", "hidden")
	payload = {key: session.get(key) for key in safe_keys if key in session}
	# SQLite stores the flags as 0/1.
	payload.update({flag: bool(payload[flag]) for flag in ("pinned", "archived", "hidden") if flag in payload})
	payload["has_system_prompt"] = bool(session.get("system_prompt"))
	payload["has_model_config"] = bool(session.get("model_config"))

	if visible is not None:
		payload["message_count"] = visible

	return payload


def _message_payload(message: Dict[str, Any]) -> Dict[str, Any]:
	"""One message, in the shape the instance's API answers with (``api_server.py::_message_response``).

	Compaction scaffolding is stripped the same way: a standalone handoff becomes a hidden empty row with a stable
	id, and a merged one keeps only the real prior-tail content.
	"""
	from agent.compaction_display import (
		_COMPACTION_INTERNAL_FIELDS, project_compaction_message_for_display)

	projected = project_compaction_message_for_display(message)

	if projected is None:
		projected = {key: value for key, value in message.items() if key not in _COMPACTION_INTERNAL_FIELDS}
		projected["content"] = ""
		projected["display_kind"] = "hidden"

	safe_keys = (
		"id", "session_id", "role", "content", "tool_call_id", "tool_calls", "tool_name",
		"timestamp", "token_count", "finish_reason", "reasoning", "reasoning_content",
		"display_kind")

	payload = {key: projected.get(key) for key in safe_keys if key in projected}

	# The row's own identity, as hex. A compaction copies a transcript into its child and the copy keeps the identity
	# the message had, which is what lets a reader of two sessions draw each message once - so it crosses the wire.
	# The store keeps it as a BLOB, and JSON has no bytes.
	identity = projected.get("display_identity")

	if identity is not None:
		payload["identity"] = bytes(identity).hex() if isinstance(identity, (bytes, bytearray, memoryview)) else str(identity)

	return payload


class FamiliarAnswerRoutes:

	async def _answer_request(self, frame: Dict[str, Any], token: str) -> None:
		"""Do what Familiar asked down the channel, and answer with the id it came with.

		Four things arrive this way. A MESSAGE is handed to this machine's own ingress, and a READ is answered from
		Hermes' own state. A SETTING changed in the app goes through the same handler the ingress uses - one
		implementation, two doors, and no second idea of what setting a model means. And UNPAIRING, which is not a
		setting on a conversation but this machine's whole relationship with that Familiar: it forgets the token. An
		answer is owed either way, because the app is waiting on this id and nothing else will settle it.
		"""
		request_id = str(frame.get("id") or "")
		channel = str(frame.get("channel") or self._home)
		action = str(frame.get("action") or "")

		try:
			if action == "say":
				result = await self._channel_say(frame)
			elif action == "api":
				# A READ rather than a setting: the app asks a PATH instead of an address, so the answer is
				# Hermes' own shape of it.
				result = await self._channel_api(frame)
			elif action == "unpair":
				# Answered here rather than through the settings table, because it is not a setting on a
				# conversation: it is this machine's whole relationship with that Familiar.
				result = self._unpair()
			else:
				known = await self._apply_action(frame, channel)
				result = {"ok": True} if known else {"error": f"unknown action: {action}"}
		except Exception as error:  # noqa: BLE001 - an answer is owed either way
			logger.warning("[%s] a channel request failed: %s", self.name, error)
			result = {"error": str(error)}

		try:
			await asyncio.to_thread(
				self._post, self._url, token, {"id": request_id, "result": result}, self.CHANNEL_REPLY_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] could not answer a channel request: %s", self.name, error)


	async def _channel_say(self, frame: Dict[str, Any]) -> Dict[str, Any]:
		"""Take a message Familiar sent, into this machine's own ingress.

		A message arriving this way IS the same thing the ingress carries, and it is handed to the same door rather
		than to a second implementation of it: the plugin posts to its own loopback ingress with its own token. So
		there is one idea of what a message from Familiar does, and the path that already works is the path used.

		The connection is the credential for reaching Familiar; inside the machine it is still the instance's own
		token, which this plugin holds.
		"""
		import aiohttp

		message = frame.get("message")

		if not isinstance(message, dict) or not str(message.get("text") or "").strip():
			return {"error": "no message"}

		timeout = aiohttp.ClientTimeout(total=30)

		async with aiohttp.ClientSession(timeout=timeout) as session:
			async with session.post(
				f"http://127.0.0.1:{self._ingress_port}{self.INGRESS_PATH}",
				headers={"Authorization": f"Bearer {self._ingress_token}"},
				json=message,
			) as response:
				try:
					body = await response.json()
				except Exception:  # noqa: BLE001 - an answer that is not JSON is still an answer
					body = {}

				if response.status >= 400:
					return {"error": f"the ingress answered HTTP {response.status}"}

				return {"sessionId": body.get("sessionId")}


	async def _channel_api(self, frame: Dict[str, Any]) -> Dict[str, Any]:
		"""Answer a path Familiar asked for, from THIS machine's own state.

		A paired machine has no address and no key, so nothing that reads an instance over HTTP applies to it: what
		used to be a request to Hermes' API arrives here as a path. The answer is that API's own shape, because the
		app parses one shape and this is the machine's honest half of it.

		A path this machine does not answer is refused BY NAME: an empty success would read as an empty
		conversation, and the reader would believe it.
		"""
		method = str(frame.get("method") or "GET").upper()
		split = urllib.parse.urlsplit(str(frame.get("path") or ""))
		path = split.path.rstrip("/") or "/"
		query = urllib.parse.parse_qs(split.query)

		for route_method, pattern, handler in _API_ROUTES:
			match = re.fullmatch(pattern, path)

			if match and route_method == method:
				return await getattr(self, handler)(match.groupdict(), query, frame.get("body"))

		return {"status": 501, "body": _api_error(f"this machine does not answer {method} {path}", "unsupported_path")}


	def _unpair(self) -> Dict[str, Any]:
		"""Forget this machine's token, which is what unpairing IS at this end.

		The token is the whole pairing: the connection is authenticated with it, a delivery or an ask carries it, and
		the ingress refuses without it. Forgotten, the channel loop finds nothing to present on its next turn and goes
		back to SHOWING A CODE - the state this machine was in before it ever paired, and the state a reader pairs it
		again from. Nothing else is touched: ``FAMILIAR_URL`` stays, so wiring this machine again is one code and no
		address at all.

		The copy the ingress was seeded with is dropped with it, unless it was configured deliberately - a value that
		is not the token being forgotten is somebody's own setting, and it is left alone.

		Synchronous and quiet: nothing here can fail in a way a reader could act on, and an answer is owed either way.
		"""
		forgotten = self._token

		self._clear_token()
		self._token = ""

		if forgotten and self._ingress_token == forgotten:
			self._ingress_token = ""

		logger.info("[familiar] this machine was unpaired from %s; it shows a code again", self._url)

		return {"ok": True, "unpaired": True}


	async def _with_session_db(self, work: Any) -> Any:
		"""Run ``work(db)`` off the event loop, against this machine's own session store.

		The store is SQLite, so it never runs on the loop, and the handle goes back to the registry rather than
		being held - which is how the API server opens it too: one store per Hermes home.
		"""
		def run() -> Any:
			from hermes_constants import get_hermes_home
			from hermes_state_registry import acquire, release_or_close

			db = acquire(Path(get_hermes_home()) / "state.db")

			try:
				return work(db)
			finally:
				release_or_close(db)

		return await asyncio.to_thread(run)


	@staticmethod
	def _bounded(query: Dict[str, List[str]], key: str, default: int, maximum: int) -> int:
		"""A page size or an offset, clamped the way the API server clamps it."""
		try:
			value = int((query.get(key) or [str(default)])[0])
		except (TypeError, ValueError):
			return default

		return default if value < 0 else min(value, maximum)


	@staticmethod
	def _no_such_session(session_id: str) -> Dict[str, Any]:
		return {"status": 404, "body": _api_error(f"Session not found: {session_id}", "session_not_found")}


	async def _api_conversation(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /familiar/conversation - which sessions one conversation is, and what they add up to.

		A conversation outlives the session it started in: the instance compresses it onto a fresh id and keeps
		`parent_session_id` for lineage. Its OWN session list hides those rows, so what a reader of that list can
		see and count is one session of a conversation rather than the conversation - which is why this walk
		happens where the store is.
		"""
		asked = [
			part.strip()
			for part in (query.get("ids") or query.get("session") or [""])[0].split(",")
			if part.strip()
		]

		if not asked:
			return {"status": 400, "body": _api_error("no session asked for", "invalid_request")}

		if len(asked) > CONVERSATION_MAX_IDS:
			return {
				"status": 400,
				"body": _api_error(f"at most {CONVERSATION_MAX_IDS} sessions per call", "invalid_request"),
			}

		# A reader that does not draw the conversation's total says so, and the store is spared the scan: the list this
		# route mostly serves draws each row's own numbers.
		totals = (query.get("totals") or ["1"])[0].strip().lower() not in ("0", "false", "no")

		return {
			"status": 200,
			"body": {
				"conversations": await self._with_session_db(lambda db: _conversations_for(db, asked, totals)),
			},
		}


	async def _api_list_sessions(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions - this machine's own sessions, most recently active first.

		``search`` is the store's own free-text filter: it matches a session's title or its id ANYWHERE IN ITS
		COMPRESSION CHAIN (so a conversation is found under a name it no longer carries, and ``an94`` finds
		``AN-94``), and it is applied in SQL BEFORE the window - which is the whole reason page two of a search is
		page two of the MATCHES rather than page two of everything. That is also why it is asked for here rather
		than filtered by the caller: a client that filters a page it was given is answering about that page only.

		``total`` is how many rows the filters admit, and it is exact whenever this machine can know it. A listing
		with no search is counted with the store's own count, built from the same WHERE its rows are. A search
		cannot be counted that way - the store counts without a search term and nothing above it counts with one -
		so this machine counts it the only way a search can be counted: by READING the matches its own filter
		admits, whose length IS the answer, up to ``_SEARCH_COUNT_CEILING``. Past that ceiling - and only past it -
		``total`` is ``None``, and a client must show what it has rather than invent a number.
		"""
		limit = self._bounded(query, "limit", 50, 200)
		offset = self._bounded(query, "offset", 0, 1_000_000)
		source = (query.get("source") or [""])[0] or None
		search = (query.get("search") or [""])[0].strip() or None
		include_children = (query.get("include_children") or [""])[0].lower() in ("1", "true", "yes")

		def read(db: Any) -> Dict[str, Any]:
			sessions = db.list_sessions_rich(
				source=source, limit=limit, offset=offset, include_children=include_children,
				order_by_last_active=True, include_pinned=True, search_query=search)
			# Pins are back-filled PAST the limit, so only the recency window decides whether another page exists.
			windowed = sum(1 for session in sessions if not session.get("pinned"))
			has_more = windowed >= limit

			if search and has_more:
				# A search that FILLS the window is the one case with no count anywhere above this machine, so the
				# matches are read: the same filter the rows just came through, asked for compact and pinned
				# included, so what comes back IS the match set and its length is the total rather than a guess.
				matches = db.list_sessions_rich(
					source=source, limit=_SEARCH_COUNT_CEILING + 1, offset=0, include_children=include_children,
					order_by_last_active=True, include_pinned=True, compact_rows=True, search_query=search)
				total = len(matches) if len(matches) <= _SEARCH_COUNT_CEILING else None
			elif search:
				# Short of the window means there is nothing after it: its own length is the answer.
				total = offset + windowed
			else:
				total = db.session_count(source=source, exclude_children=not include_children)

			return {
				"sessions": sessions,
				"has_more": has_more,
				"total": total,
				# One query for the page rather than one per row: see _visible_counts.
				"visible": _visible_counts(
					getattr(db, "_conn", None),
					[session["id"] for session in sessions if session.get("id")],
				),
			}

		page = await self._with_session_db(read)

		return {"status": 200, "body": {
			"object": "list",
			"data": [_session_payload(session, page["visible"].get(session.get("id"))) for session in page["sessions"]],
			"limit": limit, "offset": offset, "has_more": page["has_more"], "total": page["total"]}}


	async def _visible_for(self, session_id: str) -> Dict[str, int]:
		"""The visible count for one session, read from the store off the loop."""
		return await self._with_session_db(
			lambda db: _visible_counts(getattr(db, "_conn", None), [session_id]))

	async def _api_get_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions/{id} - one session, which is the cheap question "has anything changed?"."""
		session = await self._with_session_db(lambda db: db.get_session(groups["session"]))

		if not session:
			return self._no_such_session(groups["session"])

		visible = (await self._visible_for(groups["session"])).get(groups["session"])

		return {"status": 200, "body": {
			"object": "hermes.session", "session": _session_payload(session, visible)}}


	async def _api_session_messages(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions/{id}/messages - a conversation's transcript, paginated the API's own way."""
		session_id = groups["session"]
		order = (query.get("order") or [None])[0]

		if order not in (None, "oldest", "latest"):
			return {"status": 400, "body": _api_error("order must be one of: oldest, latest", "invalid_pagination")}

		raw_limit = (query.get("limit") or [None])[0]
		raw_offset = (query.get("offset") or ["0"])[0]

		try:
			offset = int(raw_offset)
			requested = None if raw_limit is None else int(raw_limit)
		except (TypeError, ValueError):
			offset = requested = -1

		if offset < 0 or (requested is not None and requested < 0):
			return {"status": 400, "body": _api_error(
				"limit and offset must be non-negative integers", "invalid_pagination")}

		# No limit asked for means the LATEST page, which is what the app reads when it opens a thread.
		default_page = requested is None
		latest_page = order == "latest" or (order is None and default_page)
		limit = 500 if default_page else min(requested, 500)

		def work(db: Any) -> Any:
			held = db.get_session(session_id)

			if not held:
				return None

			resolved = session_id

			# A session that holds NOTHING is one compression ended before its messages were flushed: what the
			# conversation says lives in the session it carried on into, so only that case follows the chain
			# (Hermes #15000). Every other session answers with its OWN transcript, because a conversation's
			# sessions are checkpoints: a reader opening one is looking for where it stopped - which is where they
			# would branch from - and resolving that to the newest session for them made every one of them the
			# same read.
			if not held.get("message_count"):
				resolved = db.resolve_resume_session_id(session_id)

			return (resolved, db.get_messages(resolved, limit=limit, offset=offset, latest=latest_page))

		answer = await self._with_session_db(work)

		if answer is None:
			return self._no_such_session(session_id)

		resolved, messages = answer

		return {"status": 200, "body": {
			"object": "list", "session_id": resolved,
			"data": [_message_payload(message) for message in messages],
			"pagination": {"limit": limit, "offset": offset,
				"order": order or ("latest" if default_page else "oldest"), "returned": len(messages)}}}


	async def _api_patch_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""PATCH /api/sessions/{id} - what the app changes about a session it did not create: its name.

		The machine's own flags are carried too, because the app relays them; a field this does not know is
		refused by name rather than accepted and ignored.
		"""
		session_id = groups["session"]
		fields = body if isinstance(body, dict) else {}
		unknown = sorted(set(fields) - {"title", "end_reason", "pinned", "archived", "hidden", "unread"})

		if unknown:
			return {"status": 400, "body": _api_error(
				f"Unsupported session fields: {', '.join(unknown)}", "unsupported_session_field")}

		for flag in ("pinned", "archived", "hidden", "unread"):
			if flag in fields and not isinstance(fields[flag], bool):
				return {"status": 400, "body": _api_error(f"'{flag}' must be a boolean", "invalid_session_field")}

		def work(db: Any) -> Any:
			if not db.get_session(session_id):
				return None

			if "title" in fields:
				db.set_session_title(session_id, "" if fields["title"] is None else str(fields["title"]))
			# Pinned last: set_session_pinned clears hidden, so a pin in the same request wins over an explicit
			# hidden - the same order the API server uses.
			for flag, setter in (("archived", db.set_session_archived), ("hidden", db.set_session_hidden),
			                     ("pinned", db.set_session_pinned)):
				if flag in fields:
					setter(session_id, fields[flag])
			if "unread" in fields:
				db.set_session_read(session_id, read=not fields["unread"])
			if fields.get("end_reason"):
				db.end_session(session_id, str(fields["end_reason"]))

			return db.get_session(session_id)

		try:
			session = await self._with_session_db(work)
		except ValueError as error:
			# A name another session already holds: the machine says which one, and it is a 400 rather than a
			# failure of the machine.
			return {"status": 400, "body": _api_error(str(error), "invalid_title")}

		if session is None:
			return self._no_such_session(session_id)

		visible = (await self._visible_for(session_id)).get(session_id)

		return {"status": 200, "body": {
			"object": "hermes.session", "session": _session_payload(session, visible)}}


	def _cron(self) -> Dict[str, Any]:
		"""This machine's own cron store, imported where the API server imports it.

		The plugin is inside Hermes, so the jobs a client creates are the same records the scheduler runs - reached
		directly rather than through an HTTP surface that has to be switched on and keyed first.
		"""
		from cron.jobs import (
			get_job, list_jobs, pause_job, remove_job, resume_job, trigger_job, update_job)
		from cron.scheduler import create_job_with_scheduler_registration

		return {"list": list_jobs, "get": get_job, "create": create_job_with_scheduler_registration,
			"update": update_job, "remove": remove_job, "pause": pause_job, "resume": resume_job,
			"trigger": trigger_job}


	@staticmethod
	def _jobs_changed() -> None:
		"""Tell a co-resident provider the store moved, the way the API's own writes do."""
		with contextlib.suppress(Exception):
			from cron.scheduler import _notify_provider_jobs_changed

			_notify_provider_jobs_changed()


	@staticmethod
	def _no_such_job() -> Dict[str, Any]:
		return {"status": 404, "body": _api_error("Job not found", "job_not_found")}


	@staticmethod
	def _job_prompt_error(prompt: str) -> str:
		"""The API's own prompt guard, so a job made here is held to what one made over HTTP is held to."""
		if len(prompt) > _MAX_JOB_PROMPT:
			return f"Prompt must be ≤ {_MAX_JOB_PROMPT} characters"

		try:
			from tools.cronjob_tools import _scan_cron_prompt
		except Exception:  # noqa: BLE001 - the scanner is optional hardening, never a reason to refuse
			return ""

		return _scan_cron_prompt(prompt) or ""


	async def _api_fork_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/sessions/{id}/fork - branch a conversation, which is how a topic starts from a session.

		The child is created FIRST and the source ended after it, which is the CLI's own order: a create that fails
		must never leave the source ended with nothing to carry on from.
		"""
		source_id = groups["session"]
		fields = body if isinstance(body, dict) else {}

		def work(db: Any) -> Any:
			source = db.get_session(source_id)

			if not source:
				return None

			fork_id = str(fields.get("id") or fields.get("session_id") or "").strip() or (
				f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}")

			if db.get_session(fork_id):
				return "exists"

			# `_branched_from` is the durable branch marker: an unmarked child is walked into its parent's
			# lineage and vanishes from a default listing.
			db.create_session(fork_id, "familiar", model=source.get("model"),
				system_prompt=source.get("system_prompt"), parent_session_id=source_id,
				model_config={"_branched_from": source_id})
			db.end_session(source_id, "branched")
			db.replace_messages(fork_id, db.get_messages(source_id))
			title = fields.get("title")
			if title is None:
				base = source.get("title") or "fork"
				title = f"{base} fork"
				with contextlib.suppress(Exception):
					title = db.get_next_title_in_lineage(base)
			db.set_session_title(fork_id, str(title))

			return db.get_session(fork_id) or {"id": fork_id, "parent_session_id": source_id}

		try:
			forked = await self._with_session_db(work)
		except ValueError as error:
			return {"status": 400, "body": _api_error(str(error), "invalid_title")}

		if forked is None:
			return self._no_such_session(source_id)

		if forked == "exists":
			return {"status": 409, "body": _api_error("Session already exists", "session_exists")}

		visible = (await self._visible_for(str(forked.get("id") or ""))).get(str(forked.get("id") or ""))

		return {"status": 201, "body": {
			"object": "hermes.session", "session": _session_payload(forked, visible)}}


	async def _api_delete_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""DELETE /api/sessions/{id} - forget a conversation on this machine."""
		session_id = groups["session"]

		def work(db: Any) -> Any:
			if not db.get_session(session_id):
				return None

			return bool(db.delete_session(session_id))

		deleted = await self._with_session_db(work)

		if deleted is None:
			return self._no_such_session(session_id)

		return {"status": 200, "body": {
			"object": "hermes.session.deleted", "id": session_id, "deleted": deleted}}


	async def _api_model_options(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/model/options - the model picker's inventory, built where Hermes builds it."""
		refresh = (query.get("refresh") or [""])[0].lower() in ("1", "true", "yes")

		def work() -> Any:
			from hermes_cli.inventory import build_model_options_payload, load_picker_context

			return build_model_options_payload(
				load_picker_context(), include_unconfigured=True, refresh=refresh)

		# Enrichment can fetch pricing and provider catalogs, which is the API server's reason for the thread too.
		return {"status": 200, "body": await asyncio.to_thread(work)}


	async def _api_list_jobs(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/jobs - what this machine has scheduled, a page at a time.

		Paged HERE, for the same reason the sessions list is: the app asks for what its table shows, so a machine with
		a hundred jobs answers with a page rather than all of them.

		NOT sorted, and the order is the instance's own. A table that holds one page cannot sort the list - it would
		sort the page and call it the answer - so the app offers no sort control here and nothing is asked for one,
		the same way the sessions and topics lists work. This machine decides the order it runs things in.
		"""
		include_disabled = (query.get("include_disabled") or [""])[0].lower() in ("true", "1")
		limit = self._bounded(query, "limit", 50, 200)
		offset = self._bounded(query, "offset", 0, 1_000_000)
		search = (query.get("search") or [""])[0].strip().lower()

		jobs = await asyncio.to_thread(
			lambda: self._cron()["list"](include_disabled=include_disabled) or [])

		if search:
			jobs = [
				job for job in jobs
				if search in str(job.get("name") or "").lower() or search in str(job.get("prompt") or "").lower()
			]

		page = jobs[offset:offset + limit]

		return {"status": 200, "body": {
			"object": "list", "data": page, "limit": limit, "offset": offset,
			"has_more": offset + len(page) < len(jobs), "total": len(jobs)}}


	async def _api_get_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/jobs/{id} - one job, as the store holds it."""
		job = await asyncio.to_thread(self._cron()["get"], groups["job"])

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()


	async def _api_create_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/jobs - schedule something on this machine.

		The job's ORIGIN is this channel, so `deliver: origin` means "wherever Familiar is" rather than an API
		server that answers nobody - the same intent a reader picks in the app, on the machine's own terms.
		"""
		fields = body if isinstance(body, dict) else {}
		name = str(fields.get("name") or "").strip()
		schedule = str(fields.get("schedule") or "").strip()
		prompt = fields.get("prompt") or ""

		if not name:
			return {"status": 400, "body": _api_error("Name is required", "invalid_job")}

		if len(name) > _MAX_JOB_NAME:
			return {"status": 400, "body": _api_error(
				f"Name must be ≤ {_MAX_JOB_NAME} characters", "invalid_job")}

		if not schedule:
			return {"status": 400, "body": _api_error("Schedule is required", "invalid_job")}

		problem = self._job_prompt_error(str(prompt))

		if problem:
			return {"status": 400, "body": _api_error(problem, "invalid_job")}

		repeat = fields.get("repeat")

		if repeat is not None and (not isinstance(repeat, int) or repeat < 1):
			return {"status": 400, "body": _api_error("Repeat must be a positive integer", "invalid_job")}

		kwargs: Dict[str, Any] = {
			"prompt": prompt, "schedule": schedule, "name": name,
			"deliver": fields.get("deliver") or "local",
			# The PLATFORM, not the label: a delivery resolves a platform by this value, and "Familiar" is
			# what the row is titled.
			"origin": {"platform": self.platform.value, "chat_id": self._home},
		}

		for key in ("paused", "paused_reason"):
			if key in fields:
				kwargs[key] = fields[key]

		if fields.get("skills"):
			kwargs["skills"] = fields["skills"]

		if repeat is not None:
			kwargs["repeat"] = repeat

		try:
			job = await asyncio.to_thread(lambda: self._cron()["create"](**kwargs))
		except ValueError as error:
			return {"status": 400, "body": _api_error(str(error), "invalid_job")}
		except Exception as error:  # noqa: BLE001 - a store that refuses says why, in its own words
			# A scheduler that cannot register the job is a 424 on the API server, and the store's own code is
			# what carries the reason - so it is passed through rather than flattened.
			status = int(getattr(error, "status", 500) or 500)

			return {"status": status, "body": _api_error(str(error), "job_create_failed")}

		return {"status": 200, "body": {"job": job}}


	async def _api_update_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""PATCH /api/jobs/{id} - what a reader changed about a job."""
		fields = body if isinstance(body, dict) else {}
		# Whitelisted the way the API server whitelists it: a field nobody may set is not silently stored.
		sanitized = {key: value for key, value in fields.items() if key in _JOB_UPDATE_FIELDS}

		if not sanitized:
			return {"status": 400, "body": _api_error("No valid fields to update", "invalid_job")}

		if "name" in sanitized and len(str(sanitized["name"])) > _MAX_JOB_NAME:
			return {"status": 400, "body": _api_error(
				f"Name must be ≤ {_MAX_JOB_NAME} characters", "invalid_job")}

		if "prompt" in sanitized:
			problem = self._job_prompt_error(str(sanitized["prompt"] or ""))

			if problem:
				return {"status": 400, "body": _api_error(problem, "invalid_job")}

		def work() -> Any:
			job = self._cron()["update"](groups["job"], sanitized)

			if job:
				self._jobs_changed()

			return job

		job = await asyncio.to_thread(work)

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()


	async def _api_delete_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""DELETE /api/jobs/{id} - take a job off the schedule."""
		def work() -> Any:
			removed = self._cron()["remove"](groups["job"])

			if removed:
				self._jobs_changed()

			return removed

		return {"status": 200, "body": {"ok": True}} if await asyncio.to_thread(work) else self._no_such_job()


	async def _api_job_action(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/jobs/{id}/run|pause|resume - one job, acted on now."""
		action = groups["action"]
		job_id = groups["job"]

		if not _JOB_ID_RE.fullmatch(job_id):
			return {"status": 400, "body": _api_error("Invalid job ID format", "invalid_job_id")}

		fields = body if isinstance(body, dict) else {}
		extra: Optional[str] = None

		if action == "run" and fields.get("prompt") is not None:
			extra = str(fields["prompt"])
			problem = self._job_prompt_error(extra)

			if problem:
				return {"status": 400, "body": _api_error(problem, "invalid_job")}

			extra = extra or None

		def work() -> Any:
			store = self._cron()

			if action == "run":
				job = store["trigger"](job_id, extra_prompt=extra)
			else:
				job = (store["pause"] if action == "pause" else store["resume"])(job_id)

			if job and action != "run":
				self._jobs_changed()

			return job

		job = await asyncio.to_thread(work)

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()
