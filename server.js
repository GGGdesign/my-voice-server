
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const PORT = process.env.PORT || 10000;
const GAME_ROOM = "maxchat_greedy_lion_global";
const GAME_PATH = "greedyLionGame";
const ROUND_MS = 25000;
const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

let firebaseReady = false;
let game = null;
let busy = false;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DATABASE_URL
  });

  firebaseReady = true;
  console.log("Firebase Admin ready");
} catch (error) {
  console.error("Firebase error:", error.message);
}

function gameRef() {
  return admin.database().ref(GAME_PATH);
}

function getHistory(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort((a, b) =>
        (a.roundNumber || 0) - (b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
}

function validGame(value) {
  return value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber > 0 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt));
}

function createGame() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

function remaining() {
  if (!game) return null;

  return Math.max(
    0,
    Math.ceil((game.roundEndsAt - Date.now()) / 1000)
  );
}

function publicState() {
  return {
    roundNumber: game ? game.roundNumber : null,
    roundStartsAt: game ? game.roundStartsAt : null,
    roundEndsAt: game ? game.roundEndsAt : null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    phase: "betting_disabled",
    bettingEnabled: false,
    multipliers: MULTIPLIERS,
    history: game ? getHistory(game.history) : []
  };
}

function broadcastState() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    publicState()
  );
}

function broadcastClock() {
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

async function updateGame() {
  if (!firebaseReady || busy) return;

  busy = true;

  try {
    const now = Date.now();
    const winner = crypto.randomInt(8);

    const transaction = await gameRef().transaction(
      current => {
        if (current === null) {
          return createGame();
        }

        if (!validGame(current)) {
          console.error(
            "Invalid game data; preserving existing data"
          );
          return;
        }

        const start = Number(current.roundStartsAt);
        const end = Number(current.roundEndsAt);
        const duration = end - start;

        const history = getHistory(current.history);

        // Repair incorrect duration without deleting history.
        if (
          duration !== ROUND_MS &&
          now < end &&
          now < start + ROUND_MS
        ) {
          return {
            ...current,
            roundEndsAt: start + ROUND_MS,
            history
          };
        }

        const correctEnd = start + ROUND_MS;

        // The old round has expired.
        if (now >= correctEnd || now >= end) {
          const result = {
            roundNumber: current.roundNumber,
            winningIndex: winner,
            completedAt: now,
            hasSettledBets: false
          };

          return {
            ...current,
            roundNumber: current.roundNumber + 1,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_MS,
            history: [...history, result].slice(-8)
          };
        }

        // Already valid: no database write needed.
        return;
      },
      undefined,
      false
    );

    const latest = transaction.snapshot.val();

    if (!validGame(latest)) {
      console.error("Invalid game state in Firebase");
      return;
    }

    const previousRound = game
      ? game.roundNumber
      : null;

    game = latest;

    if (transaction.committed) {
      console.log(
        "GAME UPDATED:",
        game.roundNumber,
        "remaining:",
        remaining()
      );
    }

    if (
      previousRound !== null &&
      game.roundNumber > previousRound
    ) {
      const result = getHistory(game.history)
        .find(item =>
          item.roundNumber === game.roundNumber - 1
        );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }
    }

    if (
      transaction.committed ||
      previousRound !== game.roundNumber
    ) {
      broadcastState();
    }

    broadcastClock();

  } catch (error) {
    console.error("GAME UPDATE ERROR:", error.message);
  } finally {
    busy = false;
  }
}

// Check Firebase every second.
setInterval(() => {
  updateGame().catch(console.error);
}, 1000);

// Existing voice chat events.
io.on("connection", socket => {
  console.log("User connected:", socket.id);

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
      { userId: data.userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      { userId: data.userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      { userId: data.userId }
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

    await updateGame();

    if (game) {
      socket.emit(
        "greedy_lion_state",
        publicState()
      );
    }
  });

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("greedy_lion_place_bet", (data, ack) => {
    if (typeof ack === "function") {
      ack({
        ok: false,
        error: "BETTING_DISABLED",
        message: "الرهانات مغلقة مؤقتًا"
      });
    }
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
  });
});

app.get("/", (req, res) => {
  const history = game
    ? getHistory(game.history)
    : [];

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: false,
    roundNumber: game ? game.roundNumber : null,
    secondsRemaining: remaining(),
    roundEndsAt: game ? game.roundEndsAt : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

server.listen(PORT, async () => {
  console.log("Server running on port", PORT);

  await updateGame();

  console.log(
    "GAME READY:",
    game ? game.roundNumber : "NOT READY"
  );
});
