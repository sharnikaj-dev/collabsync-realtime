(() => {
  "use strict";

  const editor = document.getElementById("editor");
  const editorMirror = document.getElementById("editor-mirror");
  const cursorOverlay = document.getElementById("cursor-overlay");
  const lineGutter = document.getElementById("line-gutter");
  const roomForm = document.getElementById("room-form");
  const roomInput = document.getElementById("room-input");
  const nameInput = document.getElementById("name-input");
  const activeRoomLabel = document.getElementById("active-room-label");
  const revisionNumberEl = document.getElementById("revision-number");
  const syncStateEl = document.getElementById("sync-state-label");
  const docStatsEl = document.getElementById("doc-stats");
  const participantsBar = document.getElementById("participants-bar");
  const connStatusEl = document.getElementById("connection-status");
  const connTextEl = document.getElementById("connection-text");
  const newTabLink = document.getElementById("new-tab-link");
  const toggleLogBtn = document.getElementById("toggle-log-btn");
  const protocolDrawer = document.getElementById("protocol-drawer");
  const protocolLog = document.getElementById("protocol-log");
  const clearLogBtn = document.getElementById("clear-log-btn");

  function getOrCreateTabSession(key, factory) {
    let val = sessionStorage.getItem(key);
    if (!val) {
      val = factory();
      sessionStorage.setItem(key, val);
    }
    return val;
  }

  const urlParams = new URLSearchParams(window.location.search);
  let currentRoomId = (urlParams.get("room") || "workspace-alpha").trim() || "workspace-alpha";
  let currentUserId = getOrCreateTabSession(
    "collabsync_user_id",
    () => `user-${Math.random().toString(36).slice(2, 8)}`
  );
  let currentDisplayName = getOrCreateTabSession(
    "collabsync_display_name",
    () => `Tab-${Math.floor(100 + Math.random() * 900)}`
  );

  roomInput.value = currentRoomId;
  nameInput.value = currentDisplayName;
  activeRoomLabel.textContent = currentRoomId;

  function updateNewTabLink() {
    const u = new URL(window.location.href);
    u.searchParams.set("room", currentRoomId);
    newTabLink.href = u.toString();
    window.history.replaceState(null, "", u.toString());
  }
  updateNewTabLink();

  // --- Client Sync State ---
  let ws = null;
  let reconnectTimer = null;
  let reconnectAttempts = 0;
  let heartbeatTimer = null;
  let cursorThrottleTimer = null;

  let serverVersion = 0;
  let clientSeq = 0;
  let lastKnownValue = "";
  let inFlight = null; // { client_seq, base_version, op }
  let bufferOps = []; // [{ position, delete_len, insert_text, timestamp }]
  let participants = new Map(); // user_id -> participant object
  let isApplyingRemote = false;

  // --- Splice & Minimal OT Helpers (mirrors server/rooms/ot.py) ---
  function computeSplice(prevText, nextText) {
    if (prevText === nextText) return null;
    let start = 0;
    const minLen = Math.min(prevText.length, nextText.length);
    while (start < minLen && prevText.charCodeAt(start) === nextText.charCodeAt(start)) {
      start++;
    }
    let prevEnd = prevText.length;
    let nextEnd = nextText.length;
    while (
      prevEnd > start &&
      nextEnd > start &&
      prevText.charCodeAt(prevEnd - 1) === nextText.charCodeAt(nextEnd - 1)
    ) {
      prevEnd--;
      nextEnd--;
    }
    const deleteLen = prevEnd - start;
    const insertText = nextText.slice(start, nextEnd);
    return {
      position: start,
      delete_len: deleteLen,
      insert_text: insertText,
      timestamp: Date.now() / 1000,
    };
  }

  function applySplice(text, op) {
    const pos = Math.max(0, Math.min(op.position, text.length));
    const delLen = Math.max(0, Math.min(op.delete_len, text.length - pos));
    return text.slice(0, pos) + op.insert_text + text.slice(pos + delLen);
  }

  function protocolOpToSplice(pOp) {
    if (pOp.kind === "insert") {
      return {
        position: pOp.position,
        delete_len: 0,
        insert_text: pOp.text || "",
        timestamp: pOp.timestamp || 0,
      };
    }
    if (pOp.kind === "delete") {
      return {
        position: pOp.position,
        delete_len: pOp.length || 0,
        insert_text: "",
        timestamp: pOp.timestamp || 0,
      };
    }
    return {
      position: pOp.position,
      delete_len: pOp.length || 0,
      insert_text: pOp.text || "",
      timestamp: pOp.timestamp || 0,
    };
  }

  function spliceToProtocolOp(op) {
    if (op.delete_len === 0) {
      return {
        kind: "insert",
        position: op.position,
        text: op.insert_text,
        length: 0,
        timestamp: op.timestamp,
      };
    }
    if (!op.insert_text) {
      return {
        kind: "delete",
        position: op.position,
        text: "",
        length: op.delete_len,
        timestamp: op.timestamp,
      };
    }
    return {
      kind: "replace",
      position: op.position,
      text: op.insert_text,
      length: op.delete_len,
      timestamp: op.timestamp,
    };
  }

  function transformCursorOffset(pos, op) {
    const bStart = op.position;
    const bEnd = op.position + op.delete_len;
    const bIns = op.insert_text.length;
    const bDelta = bIns - op.delete_len;
    if (pos <= bStart) return pos;
    if (pos <= bEnd) return bStart + bIns;
    return Math.max(0, pos + bDelta);
  }

  function transformOpAgainst(opA, opB) {
    const aStart = opA.position;
    const aEnd = opA.position + opA.delete_len;
    const bStart = opB.position;
    const bEnd = opB.position + opB.delete_len;
    const bIns = opB.insert_text.length;
    const bDelta = bIns - opB.delete_len;

    if (opA.delete_len === 0 && opB.delete_len === 0 && aStart === bStart) {
      return { ...opA, position: bStart + bIns };
    }
    if (aEnd <= bStart) return opA;
    if (aStart >= bEnd) {
      return { ...opA, position: Math.max(0, aStart + bDelta) };
    }
    if (opA.delete_len === 0) {
      return { ...opA, position: bStart + bIns };
    }
    if (aStart < bStart && aEnd > bEnd) {
      return {
        ...opA,
        position: aStart,
        delete_len: Math.max(0, opA.delete_len + bDelta),
      };
    }
    if (aStart >= bStart && aEnd <= bEnd) {
      return { ...opA, position: bStart + bIns, delete_len: 0 };
    }
    if (aStart < bStart && aEnd <= bEnd) {
      return { ...opA, position: aStart, delete_len: bStart - aStart };
    }
    return {
      ...opA,
      position: bStart + bIns,
      delete_len: Math.max(0, aEnd - bEnd),
    };
  }

  // --- UI Rendering ---
  function setConnectionStatus(state, label) {
    connStatusEl.className = `conn-status conn-${state}`;
    connTextEl.textContent = label;
  }

  function setRevision(ver) {
    serverVersion = ver;
    revisionNumberEl.textContent = String(ver);
  }

  function setSyncLabel(text) {
    syncStateEl.textContent = text;
  }

  function updateEditorMetrics() {
    const text = editor.value;
    const lines = text.split("\n");
    docStatsEl.textContent = `${lines.length} ${lines.length === 1 ? "line" : "lines"} · ${text.length} chars`;

    const gutterLines = [];
    for (let i = 1; i <= lines.length; i++) {
      gutterLines.push(String(i));
    }
    lineGutter.textContent = gutterLines.join("\n");

    editor.style.height = "auto";
    editor.style.height = `${Math.max(editor.scrollHeight, 480)}px`;
  }

  function offsetToLineCol(text, offset) {
    const clamped = Math.max(0, Math.min(offset, text.length));
    const slice = text.slice(0, clamped);
    const lines = slice.split("\n");
    return {
      line: lines.length,
      col: lines[lines.length - 1].length + 1,
    };
  }

  function renderParticipants() {
    participantsBar.innerHTML = "";
    const text = editor.value;
    const sorted = Array.from(participants.values()).sort((a, b) =>
      a.user_id === currentUserId ? -1 : b.user_id === currentUserId ? 1 : a.joined_at - b.joined_at
    );

    for (const p of sorted) {
      const badge = document.createElement("div");
      badge.className = "user-badge";

      const swatch = document.createElement("span");
      swatch.className = "user-swatch";
      swatch.style.backgroundColor = p.color;

      const nameEl = document.createElement("span");
      nameEl.className = "user-name";
      nameEl.textContent =
        p.user_id === currentUserId ? `${p.display_name} (You)` : p.display_name;

      const pos = p.cursor ? p.cursor.position : 0;
      const lc = offsetToLineCol(text, pos);
      const cursorEl = document.createElement("span");
      cursorEl.className = "user-cursor-pos";
      cursorEl.textContent = `Ln ${lc.line}, Col ${lc.col}`;

      badge.appendChild(swatch);
      badge.appendChild(nameEl);
      badge.appendChild(cursorEl);
      participantsBar.appendChild(badge);
    }

    renderRemoteCursors();
  }

  function renderRemoteCursors() {
    cursorOverlay.innerHTML = "";
    const text = editor.value;
    const remoteUsers = Array.from(participants.values()).filter(
      (p) => p.user_id !== currentUserId
    );
    if (remoteUsers.length === 0) return;

    // Build mirror DOM with markers at each remote cursor offset
    const markers = remoteUsers
      .map((p) => ({
        participant: p,
        offset: Math.max(0, Math.min(p.cursor ? p.cursor.position : 0, text.length)),
      }))
      .sort((a, b) => a.offset - b.offset);

    editorMirror.innerHTML = "";
    let cursorIdx = 0;
    const markerSpans = [];

    for (const item of markers) {
      if (item.offset > cursorIdx) {
        editorMirror.appendChild(
          document.createTextNode(text.slice(cursorIdx, item.offset))
        );
        cursorIdx = item.offset;
      }
      const span = document.createElement("span");
      span.textContent = "\u200b";
      editorMirror.appendChild(span);
      markerSpans.push({ span, participant: item.participant });
    }
    if (cursorIdx < text.length) {
      editorMirror.appendChild(document.createTextNode(text.slice(cursorIdx)));
    }
    if (text.endsWith("\n") || text.length === 0) {
      editorMirror.appendChild(document.createTextNode("\u200b"));
    }

    const lineHeight = parseFloat(getComputedStyle(editor).lineHeight) || 24;
    for (const { span, participant } of markerSpans) {
      const caret = document.createElement("div");
      caret.className = "remote-caret";
      caret.style.backgroundColor = participant.color;
      caret.style.height = `${lineHeight}px`;
      caret.style.transform = `translate(${span.offsetLeft}px, ${span.offsetTop}px)`;

      const flag = document.createElement("span");
      flag.className = "remote-caret-flag";
      flag.style.backgroundColor = participant.color;
      flag.textContent = participant.display_name;

      caret.appendChild(flag);
      cursorOverlay.appendChild(caret);
    }
  }

  function logFrame(direction, payload) {
    if (protocolDrawer.classList.contains("hidden") && protocolLog.childElementCount > 40) {
      protocolLog.removeChild(protocolLog.firstElementChild);
    }
    if (protocolLog.childElementCount >= 100) {
      protocolLog.removeChild(protocolLog.firstElementChild);
    }
    const entry = document.createElement("div");
    entry.className = "log-entry";

    const meta = document.createElement("div");
    meta.className = "log-meta";

    const dirSpan = document.createElement("span");
    dirSpan.className = direction === "IN" ? "log-dir-in" : "log-dir-out";
    dirSpan.textContent = `${direction} · ${payload.type}`;

    const timeSpan = document.createElement("span");
    timeSpan.textContent = new Date().toLocaleTimeString([], {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });

    meta.appendChild(dirSpan);
    meta.appendChild(timeSpan);

    const body = document.createElement("div");
    body.textContent = JSON.stringify(payload);

    entry.appendChild(meta);
    entry.appendChild(body);
    protocolLog.appendChild(entry);
    protocolLog.scrollTop = protocolLog.scrollHeight;
  }

  // --- WebSocket Transport ---
  function sendFrame(payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify(payload));
    if (payload.type !== "PING" && payload.type !== "PONG") {
      logFrame("OUT", payload);
    }
    return true;
  }

  function flushOutbox() {
    if (inFlight !== null || bufferOps.length === 0) {
      if (inFlight === null && bufferOps.length === 0) {
        setSyncLabel("Synced");
      }
      return;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setSyncLabel("Queued (Offline)");
      return;
    }

    const nextOp = bufferOps.shift();
    clientSeq += 1;
    inFlight = {
      client_seq: clientSeq,
      base_version: serverVersion,
      op: nextOp,
    };
    setSyncLabel("Syncing...");
    sendFrame({
      type: "STATE_PATCH",
      room_id: currentRoomId,
      user_id: currentUserId,
      client_seq: inFlight.client_seq,
      base_version: inFlight.base_version,
      op: spliceToProtocolOp(nextOp),
    });
  }

  function handleIncomingMessage(msg) {
    if (msg.type !== "PING" && msg.type !== "PONG") {
      logFrame("IN", msg);
    }

    switch (msg.type) {
      case "PING":
        sendFrame({ type: "PONG", ts: msg.ts });
        break;

      case "PONG":
        break;

      case "JOIN_ROOM": {
        currentUserId = msg.user_id;
        sessionStorage.setItem("collabsync_user_id", currentUserId);

        isApplyingRemote = true;
        editor.value = msg.content || "";
        lastKnownValue = editor.value;
        isApplyingRemote = false;

        inFlight = null;
        bufferOps = [];
        setRevision(msg.version || 0);
        setSyncLabel("Synced");

        participants = new Map();
        for (const p of msg.participants || []) {
          participants.set(p.user_id, p);
        }
        updateEditorMetrics();
        renderParticipants();
        break;
      }

      case "PRESENCE_UPDATE": {
        if (msg.room_id !== currentRoomId) break;
        participants = new Map();
        for (const p of msg.participants || []) {
          participants.set(p.user_id, p);
        }
        renderParticipants();
        break;
      }

      case "LEAVE_ROOM": {
        if (msg.room_id !== currentRoomId) break;
        participants.delete(msg.user_id);
        renderParticipants();
        break;
      }

      case "SYNC_ACK": {
        if (msg.room_id !== currentRoomId) break;
        if (msg.status === "resync") {
          const prevPos = editor.selectionStart;
          isApplyingRemote = true;
          editor.value = msg.content || "";
          lastKnownValue = editor.value;
          const clamped = Math.min(prevPos, editor.value.length);
          editor.setSelectionRange(clamped, clamped);
          isApplyingRemote = false;

          inFlight = null;
          bufferOps = [];
          setRevision(msg.version);
          setSyncLabel("Reconciled");
          updateEditorMetrics();
          renderRemoteCursors();
          break;
        }

        setRevision(msg.version);
        if (inFlight && msg.client_seq === inFlight.client_seq) {
          inFlight = null;
        }
        flushOutbox();
        break;
      }

      case "STATE_PATCH": {
        if (msg.room_id !== currentRoomId) break;
        setRevision(msg.version);

        const rawOps = Array.isArray(msg.ops) && msg.ops.length > 0
          ? msg.ops
          : msg.op
          ? [msg.op]
          : [];
        const remoteSplices = rawOps.map(protocolOpToSplice);

        // Transform pending local operations and incoming remote operations concurrently
        let localTransformOps = [];
        for (const rOp of remoteSplices) {
          let rForLocal = { ...rOp };
          if (inFlight) {
            const nextInFlight = transformOpAgainst(inFlight.op, rForLocal);
            rForLocal = transformOpAgainst(rForLocal, inFlight.op);
            inFlight.op = nextInFlight;
          }
          for (let i = 0; i < bufferOps.length; i++) {
            const nextBuf = transformOpAgainst(bufferOps[i], rForLocal);
            rForLocal = transformOpAgainst(rForLocal, bufferOps[i]);
            bufferOps[i] = nextBuf;
          }
          localTransformOps.push(rForLocal);
        }

        if (localTransformOps.length > 0) {
          const selStart = editor.selectionStart;
          const selEnd = editor.selectionEnd;
          let nextVal = editor.value;
          let nextStart = selStart;
          let nextEnd = selEnd;

          for (const op of localTransformOps) {
            nextVal = applySplice(nextVal, op);
            nextStart = transformCursorOffset(nextStart, op);
            nextEnd = transformCursorOffset(nextEnd, op);
          }

          isApplyingRemote = true;
          editor.value = nextVal;
          lastKnownValue = nextVal;
          editor.setSelectionRange(
            Math.min(nextStart, nextVal.length),
            Math.min(nextEnd, nextVal.length)
          );
          isApplyingRemote = false;

          updateEditorMetrics();
        }

        // Update remote author's cursor estimate if present
        const author = participants.get(msg.user_id);
        if (author && remoteSplices.length > 0) {
          const lastOp = remoteSplices[remoteSplices.length - 1];
          author.cursor = {
            position: lastOp.position + lastOp.insert_text.length,
            selection_end: lastOp.position + lastOp.insert_text.length,
          };
        }
        renderParticipants();
        break;
      }

      case "CURSOR_MOVE": {
        if (msg.room_id !== currentRoomId) break;
        const p = participants.get(msg.user_id);
        if (p) {
          p.cursor = {
            position: msg.position,
            selection_end: msg.selection_end,
          };
          renderParticipants();
        }
        break;
      }

      case "ERROR":
        setSyncLabel(`Error: ${msg.code}`);
        break;
    }
  }

  function connect() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (ws) {
      ws.onclose = null;
      ws.close();
      ws = null;
    }

    setConnectionStatus("connecting", "Connecting");
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${proto}//${window.location.host}/ws`;
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      reconnectAttempts = 0;
      setConnectionStatus("connected", "Connected");
      sendFrame({
        type: "JOIN_ROOM",
        room_id: currentRoomId,
        user_id: currentUserId,
        display_name: currentDisplayName,
      });

      if (heartbeatTimer) clearInterval(heartbeatTimer);
      heartbeatTimer = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          sendFrame({ type: "PING", ts: Date.now() / 1000 });
        }
      }, 8000);
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        handleIncomingMessage(msg);
      } catch (err) {
        console.error("Failed to parse WebSocket message", err);
      }
    };

    ws.onclose = () => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      setConnectionStatus("disconnected", "Reconnecting");
      const delay = Math.min(5000, 600 * Math.pow(1.6, reconnectAttempts));
      reconnectAttempts += 1;
      reconnectTimer = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      setConnectionStatus("disconnected", "Connection Error");
    };
  }

  function emitCursorMove() {
    const pos = editor.selectionStart || 0;
    const end = editor.selectionEnd || 0;
    const self = participants.get(currentUserId);
    if (self) {
      self.cursor = { position: pos, selection_end: end };
      renderParticipants();
    }
    if (cursorThrottleTimer) return;
    cursorThrottleTimer = setTimeout(() => {
      cursorThrottleTimer = null;
      sendFrame({
        type: "CURSOR_MOVE",
        room_id: currentRoomId,
        user_id: currentUserId,
        base_version: serverVersion,
        position: editor.selectionStart || 0,
        selection_end: editor.selectionEnd || 0,
      });
    }, 45);
  }

  // --- DOM Event Listeners ---
  editor.addEventListener("input", () => {
    if (isApplyingRemote) return;
    const currentValue = editor.value;
    const op = computeSplice(lastKnownValue, currentValue);
    lastKnownValue = currentValue;
    updateEditorMetrics();
    if (op) {
      bufferOps.push(op);
      flushOutbox();
    }
    emitCursorMove();
  });

  editor.addEventListener("keyup", emitCursorMove);
  editor.addEventListener("mouseup", emitCursorMove);
  editor.addEventListener("focus", emitCursorMove);
  window.addEventListener("resize", renderRemoteCursors);

  roomForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const nextRoom = roomInput.value.trim() || "workspace-alpha";
    const nextName = nameInput.value.trim() || currentDisplayName;
    currentDisplayName = nextName;
    sessionStorage.setItem("collabsync_display_name", currentDisplayName);

    if (nextRoom !== currentRoomId && ws && ws.readyState === WebSocket.OPEN) {
      sendFrame({
        type: "LEAVE_ROOM",
        room_id: currentRoomId,
        user_id: currentUserId,
      });
    }

    currentRoomId = nextRoom;
    activeRoomLabel.textContent = currentRoomId;
    updateNewTabLink();

    if (ws && ws.readyState === WebSocket.OPEN) {
      sendFrame({
        type: "JOIN_ROOM",
        room_id: currentRoomId,
        user_id: currentUserId,
        display_name: currentDisplayName,
      });
    } else {
      connect();
    }
  });

  toggleLogBtn.addEventListener("click", () => {
    protocolDrawer.classList.toggle("hidden");
  });

  clearLogBtn.addEventListener("click", () => {
    protocolLog.innerHTML = "";
  });

  window.addEventListener("beforeunload", () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      sendFrame({
        type: "LEAVE_ROOM",
        room_id: currentRoomId,
        user_id: currentUserId,
      });
    }
  });

  updateEditorMetrics();
  connect();
})();
