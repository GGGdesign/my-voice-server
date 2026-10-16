
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 10000;

const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";

const ROUND_MS = 25000;
const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];

// Betting stays disabled while the clock is repaired.
// This value cannot accidentally be enabled in Render.
const BETTING_ENABLED = false;

let firebaseReady = false;
let game = null;
let processing = false;
let lastSync = 0;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DB_URL
  });

  firebaseReady = true;
  console.log("FIREBASE READY");
} catch (error) {
  console.error("FIREBASE ERROR:", error.message);
}

function gameRef() {
  return admin.database().ref(GAME_PATH);
}

function validGame(value) {
  return Boolean(
    value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber >= 1 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt))
  );
}

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort(
        (a, b) =>
          Number(a.roundNumber || 0) -
          Number(b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
}

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: [],
    activeBets: {}
  };
}

function remaining() {
  if (!game) return null;

  return Math.max(
    0,
    Math.ceil(
      (Number(game.roundEndsAt) - Date.now()) / 1000
    )
  );
}

function publicState() {
  return {
    roundNumber: game ? game.roundNumber : null,
    roundStartsAt: game ? game.roundStartsAt : null,
    roundEndsAt: game ? game.roundEndsAt : null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled: BETTING_ENABLED,
    phase: "betting_disabled",
    multipliers: MULTIPLIERS,
    history: game ? historyArray(game.history) : []
  };
}

function sendState() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    publicState()
  );
}

function sendClock() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_clock",
    {
      roundNumber: game.roundNumber,
      roundStartsAt: game.roundStartsAt,
      roundEndsAt: game.roundEndsAt,
      serverTime: Date.now(),
      secondsRemaining: remaining()
    }
  );
}

// Load the saved game without changing user balances.
async function loadGame() {
  if (!firebaseReady) return;

  try {
    const snapshot = await gameRef().once("value");
    let saved = snapshot.val();

    if (saved === null) {
      const now = Date.now();

      const tx = await gameRef().transaction(
        current => {
          if (current !== null) return;
          return createGame(now);
        },
        undefined,
        false
      );

      saved = tx.snapshot.val();
    }

    if (!validGame(saved)) {
      console.error("INVALID SAVED GAME");
      return;
    }

    game = saved;
    lastSync = Date.now();

    console.log(
      "GAME LOADED:",
      game.roundNumber
    );

    sendState();
  } catch (error) {
    console.error("LOAD ERROR:", error.message);
  }
}

// This function changes ONLY greedyLionGame.
// It never locks the entire Firebase database.
async function advanceGame() {
  if (!firebaseReady || processing) return;

  processing = true;

  try {
    const now = Date.now();
    const winner = crypto.randomInt(8);

    const tx = await gameRef().transaction(
      current => {
        if (current === null) {
          return createGame(now);
        }

        if (!validGame(current)) {
          return;
        }

        const start = Number(current.roundStartsAt);
        const end = Number(current.roundEndsAt);
        const duration = end - start;

        // Repair invalid timestamps or old 250-second
        // rounds without deleting any existing data.
        if (
          duration !== ROUND_MS ||
          start > now + ROUND_MS
        ) {
          const repairedStart =
            now >= end ? now - ROUND_MS : now;

          return {
            ...current,
            roundStartsAt: repairedStart,
            roundEndsAt: repairedStart + ROUND_MS
          };
        }

        // Do nothing while the round is active.
        if (now < end) {
          return;
        }

        const roundNumber = current.roundNumber;

        const result = {
          roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        const activeBets = current.activeBets || {};

        // Preserve old unsettled bets.
        // Do not let them block the next round.
        const pendingSettlement = {
          ...(current.pendingSettlement || {})
        };

        if (Object.keys(activeBets).length > 0) {
          pendingSettlement[String(roundNumber)] = {
            roundNumber,
            winningIndex: winner,
            bets: activeBets,
            settled: false
          };
        }

        return {
          ...current,
          roundNumber: roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          activeBets: {},
          pendingSettlement,
          history: [
            ...historyArray(current.history),
            result
          ].slice(-8)
        };
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (!validGame(saved)) {
      console.error("GAME UPDATE INVALID");
      return;
    }

    const previousRound = game
      ? game.roundNumber
      : null;

    game = saved;
    lastSync = Date.now();

    if (tx.committed) {
      console.log(
        "GAME UPDATED:",
        game.roundNumber
      );
    }

    if (
      previousRound !== null &&
      game.roundNumber > previousRound
    ) {
      const completedRound =
        game.roundNumber - 1;

      const result = historyArray(game.history)
        .find(
          item =>
            item.roundNumber === completedRound
        );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log(
        "ROUND FINISHED:",
        completedRound
      );
    }

    sendState();
    sendClock();
  } catch (error) {
    console.error(
      "GAME UPDATE ERROR:",
      error.message
    );
  } finally {
    processing = false;
  }
}

// Independent clock:
// Firebase requests cannot block the timer events.
setInterval(() => {
  sendClock();

  if (!firebaseReady) return;

  if (!game) {
    if (!processing) {
      advanceGame().catch(console.error);
    }
    return;
  }

  const now = Date.now();

  const duration =
    Number(game.roundEndsAt) -
    Number(game.roundStartsAt);

  const needsUpdate =
    now >= Number(game.roundEndsAt) ||
    duration !== ROUND_MS ||
    Number(game.roundStartsAt) > now + ROUND_MS;

  // Also refresh periodically so the server can
  // recover from state changes or another instance.
  const needsSync = now - lastSync >= 10000;

  if (
    (needsUpdate || needsSync) &&
    !processing
  ) {
    advanceGame().catch(console.error);
  }
}, 1000);

// Existing voice chat events remain unchanged.
io.on("connection", socket => {
  console.log("USER CONNECTED:", socket.id);

  socket.on("join_room", data => {
    if (!data || !data.roomId) return;

    socket.join(data.roomId);

    socket.to(data.roomId).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "voice_data",
      {
        audioBuffer: data.audioBuffer,
        userId: data.userId
      }
    );
  });

  socket.on("greedy_lion_join", async () => {
    socket.join(GAME_ROOM);

    if (!game) {
      await loadGame();
    }

    if (game && socket.connected) {
      socket.emit(
        "greedy_lion_state",
        publicState()
      );

      socket.emit(
        "greedy_lion_clock",
        {
          roundNumber: game.roundNumber,
          roundStartsAt: game.roundStartsAt,
          roundEndsAt: game.roundEndsAt,
          serverTime: Date.now(),
          secondsRemaining: remaining()
        }
      );
    }
  });

  socket.on(
    "greedy_lion_place_bet",
    (data, ack) => {
      if (typeof ack === "function") {
        ack({
          ok: false,
          error: "BETTING_DISABLED",
          message:
            "الرهانات مغلقة مؤقتًا أثناء إصلاح الجولات"
        });
      }
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log(
      "USER DISCONNECTED:",
      socket.id
    );
  });
});

app.get("/", (req, res) => {
  const history = game
    ? historyArray(game.history)
    : [];

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: false,
    roundNumber: game
      ? game.roundNumber
      : null,
    secondsRemaining: remaining(),
    roundStartsAt: game
      ? game.roundStartsAt
      : null,
    roundEndsAt: game
      ? game.roundEndsAt
      : null,
    durationMs: game
      ? Number(game.roundEndsAt) -
        Number(game.roundStartsAt)
      : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

server.listen(PORT, async () => {
  console.log(
    "MAX CHAT SERVER RUNNING:",
    PORT
  );

  await loadGame();

  if (game) {
    await advanceGame();
  }

  console.log(
    "GAME READY:",
    game ? game.roundNumber : "NOT READY"
  );
});
