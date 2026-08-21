// ==================== State ====================
const token = localStorage.getItem('token');
const state = {
  token: token,
  username: localStorage.getItem('username'),
  userId: null,
  currentRoomId: null,
  ws: null,
  rooms: [],
  onlineUsers: new Map(),
  typingTimeouts: new Map(),
  myTypingActive: false,
  myTypingTimer: null,
  oldestMessageId: null,
};

// ==================== API Helpers ====================
async function api(method, path, body = null) {
  const headers = { 'Authorization': `Bearer ${state.token}` };
  if (body) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(body);
  }
  const res = await fetch(path, { method, headers, body });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(detail.detail || 'Request failed');
  }
  return res.json();
}

async function apiForm(method, path, formData) {
  const res = await fetch(path, { method, body: formData });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(detail.detail || 'Request failed');
  }
  return res.json();
}

// ==================== Toast ====================
function toast(msg, isError = false) {
  const el = document.createElement('div');
  el.className = `toast${isError ? ' error' : ''}`;
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => { el.remove(); }, 4000);
}

// ==================== Auth ====================
function showAuth() {
  document.getElementById('auth-view').classList.remove('hidden');
  document.getElementById('chat-view').classList.add('hidden');
}

function showChat() {
  document.getElementById('auth-view').classList.add('hidden');
  document.getElementById('chat-view').classList.remove('hidden');
  loadRooms();
}

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('login-form').classList.toggle('hidden', tab.dataset.tab !== 'login');
    document.getElementById('register-form').classList.toggle('hidden', tab.dataset.tab !== 'register');
    document.getElementById('auth-error').classList.add('hidden');
  });
});

document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const errEl = document.getElementById('auth-error');
  try {
    const fd = new FormData(form);
    const data = await apiForm('POST', '/api/auth/login', fd);
    localStorage.setItem('token', data.access_token);
    state.token = data.access_token;
    state.username = form.username.value;
    localStorage.setItem('username', state.username);
    showChat();
  } catch (err) {
    errEl.textContent = err.message;
    errEl.classList.remove('hidden');
  }
});

document.getElementById('register-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const errEl = document.getElementById('auth-error');
  try {
    await api('POST', '/api/auth/register', {
      username: form.username.value,
      email: form.email.value,
      password: form.password.value,
    });
    // Auto-login after register
    const fd = new FormData();
    fd.append('username', form.username.value);
    fd.append('password', form.password.value);
    const data = await apiForm('POST', '/api/auth/login', fd);
    localStorage.setItem('token', data.access_token);
    state.token = data.access_token;
    state.username = form.username.value;
    localStorage.setItem('username', state.username);
    showChat();
  } catch (err) {
    errEl.textContent = err.message;
    errEl.classList.remove('hidden');
  }
});

document.getElementById('logout-btn').addEventListener('click', () => {
  if (state.ws) state.ws.close();
  localStorage.removeItem('token');
  localStorage.removeItem('username');
  state.token = null;
  state.username = null;
  state.currentRoomId = null;
  showAuth();
});

// ==================== Rooms ====================
async function loadRooms() {
  try {
    state.rooms = await api('GET', '/api/rooms');
    renderRooms();
  } catch (err) {
    toast('Failed to load rooms: ' + err.message, true);
  }
}

function renderRooms() {
  const list = document.getElementById('room-list');
  list.innerHTML = '';
  for (const room of state.rooms) {
    const el = document.createElement('div');
    el.className = 'room-item';
    if (room.id === state.currentRoomId) el.classList.add('active');
    el.textContent = room.name;
    el.addEventListener('click', () => switchRoom(room.id, room.name));
    list.appendChild(el);
  }
}

document.getElementById('create-room-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const input = document.getElementById('room-name-input');
  try {
    const room = await api('POST', '/api/rooms', { name: input.value, is_private: false });
    state.rooms.push(room);
    input.value = '';
    renderRooms();
    switchRoom(room.id, room.name);
  } catch (err) {
    toast(err.message, true);
  }
});

async function switchRoom(roomId, roomName) {
  // Close old WS and clear stale typing state
  if (state.ws) {
    state.ws.close();
    state.ws = null;
  }
  state.myTypingActive = false;
  if (state.myTypingTimer) {
    clearTimeout(state.myTypingTimer);
    state.myTypingTimer = null;
  }
  state.typingTimeouts.forEach(t => clearTimeout(t));
  state.typingTimeouts.clear();
  document.getElementById('typing-indicator').classList.add('hidden');
  // If not a member, try to join
  const room = state.rooms.find(r => r.id === roomId);
  if (!room) {
    try {
      const joined = await api('POST', `/api/rooms/${roomId}/join`);
      state.rooms.push(joined);
    } catch (err) {
      toast('Cannot join room: ' + err.message, true);
      return;
    }
  }

  state.currentRoomId = roomId;
  document.getElementById('current-room').textContent = '#' + roomName;
  document.getElementById('message-input').disabled = false;
  document.getElementById('send-btn').disabled = false;

  // Clear messages and online users
  document.getElementById('messages').innerHTML = '';
  state.onlineUsers.clear();
  state.oldestMessageId = null;
  renderOnlineList();

  // Load message history
  await loadMessages(roomId);

  // Connect WebSocket
  connectWS(roomId);
  renderRooms();
  closeSidebar();
}

// ==================== Messages ====================
async function loadMessages(roomId, before = null) {
  try {
    let path = `/api/rooms/${roomId}/messages?limit=50`;
    if (before) path += `&before=${before}`;
    const msgs = await api('GET', path);
    const container = document.getElementById('messages');

    // API returns newest-first (id DESC). Capture the oldest message ID
    // for cursor-based pagination BEFORE reversing for display.
    const originalLength = msgs.length;
    const oldestId = originalLength > 0 ? msgs[originalLength - 1].id : null;

    if (before) {
      // Prepend older messages (reverse to get oldest-first for DOM order)
      const scrollHeightBefore = container.scrollHeight;
      const reversed = [...msgs].reverse();
      for (const msg of reversed) {
        container.prepend(renderMessage(msg));
      }
      // Maintain scroll position
      container.scrollTop += container.scrollHeight - scrollHeightBefore;
    } else {
      const reversed = [...msgs].reverse();
      for (const msg of reversed) {
        container.appendChild(renderMessage(msg));
      }
      scrollToBottom();
    }

    if (oldestId !== null) {
      state.oldestMessageId = oldestId;
      document.getElementById('load-more-wrap').classList.remove('hidden');
    } else {
      document.getElementById('load-more-wrap').classList.add('hidden');
    }

    if (originalLength < 50) {
      document.getElementById('load-more-wrap').classList.add('hidden');
    }
  } catch (err) {
    toast('Failed to load messages: ' + err.message, true);
  }
}

document.getElementById('load-more-btn').addEventListener('click', () => {
  if (state.oldestMessageId && state.currentRoomId) {
    loadMessages(state.currentRoomId, state.oldestMessageId);
  }
});

function renderMessage(msg) {
  const el = document.createElement('div');
  el.className = 'message';

  const header = document.createElement('div');
  header.className = 'message-header';

  const author = document.createElement('span');
  author.className = 'message-author';
  if (msg.username === state.username) author.classList.add('self');
  author.textContent = msg.username;
  header.appendChild(author);

  const time = document.createElement('span');
  time.className = 'message-time';
  time.textContent = formatTime(msg.created_at);
  header.appendChild(time);
  el.appendChild(header);

  const content = document.createElement('div');
  content.className = 'message-content';
  content.textContent = msg.content;
  el.appendChild(content);

  return el;
}

function formatTime(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

// ==================== WebSocket ====================
function connectWS(roomId) {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${window.location.host}/ws/chat/${roomId}?token=${state.token}`;
  state.ws = new WebSocket(url);

  state.ws.onopen = () => {};

  state.ws.onmessage = (evt) => {
    const event = JSON.parse(evt.data);
    handleEvent(event);
  };

  state.ws.onclose = () => {
    // Could implement reconnect here
  };

  state.ws.onerror = () => {
    toast('WebSocket connection error', true);
  };
}

function handleEvent(event) {
  switch (event.type) {
    case 'message':
      appendMessage(event);
      break;
    case 'join':
      appendSystem(`${event.username} joined the room`);
      break;
    case 'leave':
      appendSystem(`${event.username} left the room`);
      clearTyping(event.username);
      break;
    case 'typing':
      showTyping(event.username);
      break;
    case 'presence':
      updateOnlineUsers(event.online_users);
      break;
    case 'error':
      toast(event.content, true);
      break;
  }
}

function appendMessage(event) {
  const container = document.getElementById('messages');
  container.appendChild(renderMessage({
    username: event.username,
    content: event.content,
    created_at: event.created_at,
  }));
  if (isScrolledToBottom()) {
    scrollToBottom();
  }
}

function appendSystem(text) {
  const container = document.getElementById('messages');
  const el = document.createElement('div');
  el.className = 'message-system';
  el.textContent = text;
  container.appendChild(el);
  if (isScrolledToBottom()) {
    scrollToBottom();
  }
}

// ==================== Typing ====================
function showTyping(username) {
  if (username === state.username) return;

  // Reset timer
  if (state.typingTimeouts.has(username)) {
    clearTimeout(state.typingTimeouts.get(username));
  }

  // Show indicator
  const indicator = document.getElementById('typing-indicator');
  indicator.querySelector('.typing-text').textContent = `${username} is typing`;
  indicator.classList.remove('hidden');

  // Set timeout to hide
  state.typingTimeouts.set(username, setTimeout(() => {
    clearTyping(username);
  }, 3000));
}

function clearTyping(username) {
  if (state.typingTimeouts.has(username)) {
    clearTimeout(state.typingTimeouts.get(username));
    state.typingTimeouts.delete(username);
  }
  if (state.typingTimeouts.size === 0) {
    document.getElementById('typing-indicator').classList.add('hidden');
  } else {
    // Show next typing user
    const next = state.typingTimeouts.keys().next().value;
    document.getElementById('typing-indicator').querySelector('.typing-text').textContent = `${next} is typing`;
  }
}

// Send typing events (debounced)
document.getElementById('message-input').addEventListener('input', () => {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  if (state.myTypingActive) return;

  state.myTypingActive = true;
  state.ws.send(JSON.stringify({ type: 'typing' }));

  // Reset after 2s of silence
  state.myTypingTimer = setTimeout(() => {
    state.myTypingActive = false;
  }, 2000);
});

// ==================== Presence ====================
function updateOnlineUsers(onlineMap) {
  state.onlineUsers = new Map(Object.entries(onlineMap));
  renderOnlineList();

  // Update member count
  const count = state.onlineUsers.size;
  document.getElementById('member-count').textContent = `${count} online`;
}

function renderOnlineList() {
  const list = document.getElementById('online-list');
  list.innerHTML = '';
  for (const [id, username] of state.onlineUsers) {
    const el = document.createElement('div');
    el.className = 'online-user';
    const dot = document.createElement('span');
    dot.className = 'online-dot';
    const name = document.createElement('span');
    name.textContent = username;
    if (username === state.username) {
      name.style.fontWeight = '600';
      name.style.color = 'var(--accent)';
    }
    el.appendChild(dot);
    el.appendChild(name);
    list.appendChild(el);
  }
}

// ==================== Message Send ====================
document.getElementById('message-form').addEventListener('submit', (e) => {
  e.preventDefault();
  const input = document.getElementById('message-input');
  const content = input.value.trim();
  if (!content || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;

  state.ws.send(JSON.stringify({ type: 'message', content }));
  input.value = '';

  // Reset typing state
  state.myTypingActive = false;
  if (state.myTypingTimer) clearTimeout(state.myTypingTimer);
});

// ==================== Scroll Helpers ====================
function isScrolledToBottom() {
  const container = document.getElementById('messages');
  return container.scrollHeight - container.scrollTop - container.clientHeight < 50;
}

function scrollToBottom() {
  const container = document.getElementById('messages');
  container.scrollTop = container.scrollHeight;
}

// ==================== Mobile Sidebar Drawer ====================
function openSidebar() {
  document.getElementById('sidebar')?.classList.add('open');
  document.getElementById('sidebar-overlay')?.classList.add('open');
}

function closeSidebar() {
  document.getElementById('sidebar')?.classList.remove('open');
  document.getElementById('sidebar-overlay')?.classList.remove('open');
}

document.getElementById('sidebar-toggle')?.addEventListener('click', openSidebar);
document.getElementById('sidebar-close')?.addEventListener('click', closeSidebar);
document.getElementById('sidebar-overlay')?.addEventListener('click', closeSidebar);

// ==================== Init ====================
if (state.token && state.username) {
  showChat();
} else {
  showAuth();
}

