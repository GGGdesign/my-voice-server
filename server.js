
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
const ALLOWED_AMOUNTS = new Set([
  100, 500, 1000, 5000, 10000
]);

const MAX_SLOTS = 6;
const MAX_BETS_PER_ROUND = 2000;

// Keep disabled until wallet settlement is tested.
const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS === "true";

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

function historyArray(value) {
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
  return Boolean(
    value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber > 0 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt))
  );
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

function asCoins(value) {
  const number =
    typeof value === "string" &&
    /^\d+$/.test(value)
      ? Number(value)
      : value;

  return Number.isSafeInteger(number) &&
    number >= 0
      ? number
      : null;
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
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    bettingEnabled: BETTING_ENABLED,
    multipliers: MULTIPLIERS,
    history: game ? historyArray(game.history) : []
  };
}

function sendState() {
  if (game) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_state",
      publicState()
    );
  }
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

async function updateGame() {
  if (!firebaseReady || busy) return;

  busy = true;

  try {
    const now = Date.now();
    const winner = crypto.randomInt(8);

    const target = BETTING_ENABLED
      ? admin.database().ref()
      : gameRef();

    const tx = await target.transaction(
      rootOrGame => {
        if (rootOrGame === null) {
          if (BETTING_ENABLED) return;
          return createGame();
        }

        const root = BETTING_ENABLED
          ? rootOrGame
          : null;

        const current = BETTING_ENABLED
          ? root[GAME_PATH]
          : rootOrGame;

        if (!validGame(current)) return;

        const start = Number(current.roundStartsAt);
        const end = Number(current.roundEndsAt);
        const correctedEnd = start + ROUND_MS;

        // Preserve the working 25-second timing.
        if (now < correctedEnd && now < end) {
          if (end === correctedEnd) return;

          const repaired = {
            ...current,
            roundEndsAt: correctedEnd
          };

          if (BETTING_ENABLED) {
            root[GAME_PATH] = repaired;
            return root;
          }

          return repaired;
        }

        const result = {
          roundNumber: current.roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: BETTING_ENABLED
        };

        const next = {
          ...current,
          roundNumber: current.roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          history: [
            ...historyArray(current.history),
            result
          ].slice(-8)
        };

        if (!BETTING_ENABLED) {
          return next;
        }

        // Atomically settle winnings in the same
        // database transaction as the round result.
        const active = current.activeBets || {};
        const bets = Object.values(active);

        if (bets.length > MAX_BETS_PER_ROUND) {
          return;
        }

        const winnings = {};

        for (const bet of bets) {
          if (!bet || bet.foodIndex !== winner) {
            continue;
          }

          const amount = asCoins(bet.amount);

          if (amount === null) return;

          const payout =
            amount * MULTIPLIERS[winner];

          if (!Number.isSafeInteger(payout)) {
            return;
          }

          winnings[bet.uid] =
            (winnings[bet.uid] || 0) + payout;

          if (
            !Number.isSafeInteger(winnings[bet.uid])
          ) {
            return;
          }
        }

        for (const [uid, payout] of
          Object.entries(winnings)) {
          const user =
            root.Users && root.Users[uid];

          const coins =
            user && asCoins(user.coins);

          if (
            coins === null ||
            !Number.isSafeInteger(coins + payout)
          ) {
            return;
          }
        }

        for (const [uid, payout] of
          Object.entries(winnings)) {
          root.Users[uid].coins += payout;
        }

        result.winnings = winnings;
        next.activeBets = {};

        root[GAME_PATH] = next;
        return root;
      },
      undefined,
      false
    );

    const value = tx.snapshot.val();

    const latest = BETTING_ENABLED
      ? value && value[GAME_PATH]
      : value;

    if (!validGame(latest)) return;

    const previousRound = game
      ? game.roundNumber
      : null;

    game = latest;

    if (
      previousRound !== null &&
      game.roundNumber > previousRound
    ) {
      const result = historyArray(game.history)
        .find(item =>
          item.roundNumber === game.roundNumber - 1
        );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log(
        "ROUND FINISHED:",
        previousRound
      );
    }

    if (
      tx.committed ||
      previousRound !== game.roundNumber
    ) {
      sendState();
    }

    sendClock();
  } catch (error) {
    console.error(
      "GAME UPDATE ERROR:",
      error.message
    );
  } finally {
    busy = false;
  }
}

async function placeBet(payload) {
  if (!BETTING_ENABLED) {
    return {
      ok: false,
      error: "BETTING_DISABLED"
    };
  }

  if (!firebaseReady) {
    return {
      ok: false,
      error: "SERVER_UNAVAILABLE"
    };
  }

  if (!payload || typeof payload !== "object") {
    return {
      ok: false,
      error: "INVALID_REQUEST"
    };
  }

  const {
    idToken,
    betId,
    foodIndex,
    amount,
    roundNumber
  } = payload;

  if (
    typeof idToken !== "string" ||
    idToken.length > 5000 ||
    typeof betId !== "string" ||
    !/^[a-fA-F0-9]{32}$/.test(betId) ||
    !Number.isInteger(foodIndex) ||
    foodIndex < 0 ||
    foodIndex > 7 ||
    !ALLOWED_AMOUNTS.has(amount) ||
    !Number.isSafeInteger(roundNumber)
  ) {
    return {
      ok: false,
      error: "INVALID_BET"
    };
  }

  let uid;

  try {
    const decoded =
      await admin.auth().verifyIdToken(
        idToken,
        true
      );

    uid = decoded.uid;
  } catch (error) {
    return {
      ok: false,
      error: "AUTH_FAILED"
    };
  }

  const now = Date.now();
  let rejection = "BET_REJECTED";

  try {
    const tx = await admin.database()
      .ref()
      .transaction(root => {
        if (
          !root ||
          !validGame(root[GAME_PATH])
        ) {
          rejection = "GAME_UNAVAILABLE";
          return;
        }

        const current = root[GAME_PATH];

        if (
          current.roundNumber !== roundNumber ||
          now >= Number(current.roundEndsAt) - 1000 ||
          now < Number(current.roundStartsAt)
        ) {
          rejection = "ROUND_CLOSED";
          return;
        }

        const user =
          root.Users && root.Users[uid];

        const coins =
          user && asCoins(user.coins);

        if (coins === null) {
          rejection = "INVALID_WALLET";
          return;
        }

        const active =
          current.activeBets || {};

        const key = uid + "_" + betId;

        if (active[key]) {
          rejection = "DUPLICATE_BET";
          return;
        }

        if (
          Object.keys(active).length >=
          MAX_BETS_PER_ROUND
        ) {
          rejection = "ROUND_FULL";
          return;
        }

        const playerBets =
          Object.values(active).filter(
            bet => bet && bet.uid === uid
          );

        const slots = new Set(
          playerBets.map(bet => bet.foodIndex)
        );

        if (
          !slots.has(foodIndex) &&
          slots.size >= MAX_SLOTS
        ) {
          rejection = "MAX_SLOTS";
          return;
        }

        if (coins < amount) {
          rejection = "INSUFFICIENT_COINS";
          return;
        }

        // Debit and record the bet atomically.
        user.coins = coins - amount;

        active[key] = {
          uid,
          betId,
          foodIndex,
          amount,
          roundNumber
        };

        current.activeBets = active;

        return root;
      }, undefined, false);

    if (tx.committed) {
      return {
        ok: true,
        betId,
        amount,
        foodIndex,
        roundNumber
      };
    }

    return {
      ok: false,
      error: rejection
    };
  } catch (error) {
    console.error(
      "BET ERROR:",
      error.message
    );

    return {
      ok: false,
      error: "SERVER_ERROR"
    };
  }
}

// Keep the game clock running.
setInterval(() => {
  sendClock();
  updateGame().catch(console.error);
}, 1000);

// Preserve existing voice chat functionality.
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

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const response = await placeBet(data);

      if (typeof ack === "function") {
        ack(response);
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
  const history = game
    ? historyArray(game.history)
    : [];

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber: game
      ? game.roundNumber
      : null,
    secondsRemaining: remaining(),
    roundEndsAt: game
      ? game.roundEndsAt
      : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

server.listen(PORT, async () => {
  console.log(
    "Max Chat server running:",
    PORT
  );

  await updateGame();

  console.log(
    "GAME READY:",
    game ? game.roundNumber : "NOT READY"
  );
});
