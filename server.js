
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const { Server } = require("socket.io");
const admin = require("firebase-admin");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_ROOM = "maxchat_greedy_lion_global";
const GAME_PATH = "greedyLionGame";
const ROUND_MS = 25000;

const FOOD_MULTIPLIERS = [
  45, 5, 5, 10, 15, 5, 5, 25
];

const BETTING_ENABLED = false;

let firebaseReady = false;
let gameState = null;
let saving = false;
let pendingSave = false;

try {
  if (fs.existsSync(SECRET_PATH)) {
    const credentials = JSON.parse(
      fs.readFileSync(SECRET_PATH, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(credentials),
      databaseURL: DATABASE_URL
    });

    firebaseReady = true;
    console.log("Firebase Admin ready");
  } else {
    console.error("Firebase secret missing");
  }
} catch (error) {
  console.error(
    "Firebase initialization error:",
    error.message
  );
}

function initialState() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

function getHistory(state) {
  return Array.isArray(state?.history)
    ? state.history
    : [];
}

function validState(state) {
  return (
    state &&
    typeof state === "object" &&
    Number.isSafeInteger(state.roundNumber) &&
    state.roundNumber > 0 &&
    Number.isFinite(state.roundStartsAt) &&
    Number.isFinite(state.roundEndsAt) &&
    state.roundEndsAt > state.roundStartsAt
  );
}

function publicState() {
  if (!gameState) return null;

  return {
    roundNumber: gameState.roundNumber,
    roundStartsAt: gameState.roundStartsAt,
    roundEndsAt: gameState.roundEndsAt,
    serverTime: Date.now(),
    phase: "betting_disabled",
    bettingEnabled: false,
    history: getHistory(gameState).slice(-8),
    multipliers: FOOD_MULTIPLIERS
  };
}

function sendClock(target) {
  if (!gameState) return;

  target.emit("greedy_lion_clock", {
    roundNumber: gameState.roundNumber,
    roundEndsAt: gameState.roundEndsAt,
    serverTime: Date.now()
  });
}

function sendState(target) {
  const state = publicState();

  if (state) {
    target.emit("greedy_lion_state", state);
  }
}

// Save game state without blocking the timer.
// This server assumes one active Render instance.
async function saveState() {
  if (!firebaseReady || !gameState) return;

  if (saving) {
    pendingSave = true;
    return;
  }

  saving = true;

  try {
    do {
      pendingSave = false;

      const snapshot = JSON.parse(
        JSON.stringify(gameState)
      );

      await admin.database()
        .ref(GAME_PATH)
        .set(snapshot);

      console.log(
        "ROUND SAVED:",
        snapshot.roundNumber
      );
    } while (pendingSave);
  } catch (error) {
    console.error(
      "FIREBASE SAVE ERROR:",
      error.message
    );
  } finally {
    saving = false;
  }
}

// The game clock is independent of Firebase.
function finishRound() {
  if (!gameState) return;

  const now = Date.now();

  if (now < gameState.roundEndsAt) {
    return;
  }

  const completedRound = gameState.roundNumber;
  const winner = crypto.randomInt(8);

  const result = {
    roundNumber: completedRound,
    winningIndex: winner,
    completedAt: now,
    hasSettledBets: false
  };

  gameState.history = [
    ...getHistory(gameState),
    result
  ].slice(-8);

  gameState.roundNumber += 1;
  gameState.roundStartsAt = now;
  gameState.roundEndsAt = now + ROUND_MS;

  console.log(
    "ROUND FINISHED:",
    completedRound,
    "WINNER:",
    winner
  );

  console.log(
    "ROUND STARTED:",
    gameState.roundNumber
  );

  io.to(GAME_ROOM).emit(
    "greedy_lion_round_result",
    result
  );

  sendState(io.to(GAME_ROOM));
  sendClock(io.to(GAME_ROOM));

  // Saving happens in the background.
  saveState().catch(error => {
    console.error(
      "SAVE ERROR:",
      error.message
    );
  });
}

function gameTick() {
  if (!gameState) return;

  finishRound();

  sendClock(io.to(GAME_ROOM));
}

setInterval(gameTick, 1000);

async function initializeGame() {
  let loaded = null;

  if (firebaseReady) {
    try {
      const snapshot = await admin.database()
        .ref(GAME_PATH)
        .once("value");

      loaded = snapshot.val();
    } catch (error) {
      console.error(
        "FIREBASE LOAD ERROR:",
        error.message
      );
    }
  }

  if (validState(loaded)) {
    gameState = {
      roundNumber: loaded.roundNumber,
      roundStartsAt: loaded.roundStartsAt,
      roundEndsAt: loaded.roundEndsAt,
      history: getHistory(loaded).slice(-8)
    };

    console.log(
      "Loaded saved round:",
      gameState.roundNumber
    );
  } else {
    gameState = initialState();

    console.log(
      "Starting new game clock"
    );
  }

  // Recover from an expired saved round.
  if (Date.now() >= gameState.roundEndsAt) {
    finishRound();
  }

  console.log(
    "GAME CLOCK READY:",
    gameState.roundNumber
  );

  sendState(io.to(GAME_ROOM));
  sendClock(io.to(GAME_ROOM));
}

io.on("connection", socket => {
  console.log(
    "User connected:",
    socket.id
  );

  // Existing voice chat events

  socket.on("join_room", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, userName } = data;

    socket.join(roomId);

    socket.to(roomId).emit(
      "user_joined_room",
      { userId, userName }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, isMuted } = data;

    io.to(roomId).emit(
      "user_toggle_mute",
      { userId, isMuted }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, seatIndex } = data;

    io.to(roomId).emit(
      "user_sat_on_seat",
      { userId, seatIndex }
    );
  });

  socket.on("leave_seat", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    io.to(roomId).emit(
      "user_left_seat",
      { roomId, userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_started_audio",
      { roomId, userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_stopped_audio",
      { roomId, userId }
    );
  });

  socket.on("voice_data", data => {
    if (!data || !data.roomId) return;

    const { roomId, audioBuffer, userId } = data;

    socket.to(roomId).emit(
      "voice_data",
      { roomId, audioBuffer, userId }
    );
  });

  // Greedy Lion events

  socket.on("greedy_lion_join", () => {
    socket.join(GAME_ROOM);

    sendState(socket);
    sendClock(socket);

    console.log(
      "Greedy Lion player joined"
    );
  });

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on(
    "greedy_lion_place_bet",
    (data, ack) => {
      if (typeof ack === "function") {
        ack({
          ok: false,
          error: "BETTING_DISABLED",
          message:
            "الرهانات مغلقة مؤقتًا لحماية الرصيد"
        });
      }
    }
  );

  socket.on("disconnect", () => {
    console.log(
      "User disconnected:",
      socket.id
    );
  });
});

app.get("/", (req, res) => {
  res.set("Cache-Control", "no-store");

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber:
      gameState?.roundNumber ?? null,
    secondsRemaining: gameState
      ? Math.max(
          0,
          Math.ceil(
            (gameState.roundEndsAt - Date.now()) /
              1000
          )
        )
      : null,
    lastResult:
      getHistory(gameState).slice(-1)[0] ??
      null
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, async () => {
  console.log(
    "Max Chat server running on port",
    PORT
  );

  await initializeGame();
});
