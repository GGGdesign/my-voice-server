
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
const CHIPS = [100, 500, 1000, 5000, 10000];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS === "true";

let firebaseReady = false;
let game = null;

let roundProcessing = false;
let settlementProcessing = false;
let loadProcessing = false;

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
    activeBets: {},
    pendingSettlement: {},
    history: []
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
    bettingEnabled: BETTING_ENABLED && firebaseReady,
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
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

async function loadGame() {
  if (!firebaseReady || loadProcessing) return;

  loadProcessing = true;

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
      console.error("INVALID GAME DATA");
      return;
    }

    game = saved;
    lastSync = Date.now();

    console.log("GAME LOADED:", game.roundNumber);

    sendState();
  } catch (error) {
    console.error("LOAD ERROR:", error.message);
  } finally {
    loadProcessing = false;
  }
}

/*
  ROUND ENGINE

  Only updates greedyLionGame.

  Bets from the finished round are moved to
  pendingSettlement.

  Wallet settlement runs separately and cannot
  intentionally hold the round timer.
*/

async function advanceGame() {
  if (!firebaseReady || roundProcessing) return;

  roundProcessing = true;

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

        if (
          duration !== ROUND_MS ||
          start > now + ROUND_MS
        ) {
          return {
            ...current,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_MS
          };
        }

        if (Date.now() < end) {
          return;
        }

        const roundNumber = current.roundNumber;
        const activeBets = current.activeBets || {};

        const result = {
          roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        const pendingSettlement = {
          ...(current.pendingSettlement || {})
        };

        if (Object.keys(activeBets).length > 0) {
          pendingSettlement[String(roundNumber)] = {
            roundNumber,
            winningIndex: winner,
            completedAt: now,
            payoutVersion: 1,
            settled: false,
            bets: activeBets
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

    if (!validGame(saved)) return;

    const previousRound = game
      ? game.roundNumber
      : null;

    game = saved;
    lastSync = Date.now();

    if (
      previousRound !== null &&
      game.roundNumber > previousRound
    ) {
      const finishedRound = game.roundNumber - 1;

      const result = historyArray(game.history)
        .find(
          item => item.roundNumber === finishedRound
        );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log("ROUND FINISHED:", finishedRound);
    }

    sendState();
    sendClock();
  } catch (error) {
    console.error("ROUND ERROR:", error.message);
  } finally {
    roundProcessing = false;
  }
}

/*
  BETTING

  Firebase ID token identifies the user.

  A root transaction atomically records the bet
  and subtracts coins.

  This transaction runs only when a user bets,
  never every second.
*/

async function placeBet(data) {
  if (!BETTING_ENABLED) {
    return {
      ok: false,
      error: "BETTING_DISABLED",
      message: "الرهانات مغلقة"
    };
  }

  if (!firebaseReady || !game) {
    return {
      ok: false,
      error: "SERVER_NOT_READY"
    };
  }

  const token = String(data?.idToken || "");
  const betId = String(data?.betId || "");

  const foodIndex = Number(data?.foodIndex);
  const amount = Number(data?.amount);
  const roundNumber = Number(data?.roundNumber);

  if (
    !/^[a-f0-9]{32}$/i.test(betId) ||
    !Number.isInteger(foodIndex) ||
    foodIndex < 0 ||
    foodIndex > 7 ||
    !CHIPS.includes(amount) ||
    !Number.isSafeInteger(roundNumber)
  ) {
    return {
      ok: false,
      error: "INVALID_BET"
    };
  }

  let decoded;

  try {
    decoded = await admin.auth().verifyIdToken(token);
  } catch (error) {
    return {
      ok: false,
      error: "INVALID_TOKEN"
    };
  }

  const uid = decoded.uid;

  const betKey = `${uid}_${betId}`;

  let rejection = "BET_REJECTED";

  try {
    const tx = await admin.database().ref()
      .transaction(
        root => {
          if (!root) {
            rejection = "DATABASE_EMPTY";
            return;
          }

          const current = root[GAME_PATH];

          if (!validGame(current)) {
            rejection = "GAME_NOT_READY";
            return;
          }

          if (
            current.roundNumber !== roundNumber ||
            Date.now() >= Number(current.roundEndsAt)
          ) {
            rejection = "ROUND_CLOSED";
            return;
          }

          const users = root.Users || {};
          const user = users[uid];

          if (!user) {
            rejection = "USER_NOT_FOUND";
            return;
          }

          const coins = Number(user.coins);

          if (
            !Number.isSafeInteger(coins) ||
            coins < amount
          ) {
            rejection = "INSUFFICIENT_COINS";
            return;
          }

          const activeBets = {
            ...(current.activeBets || {})
          };

          if (activeBets[betKey]) {
            rejection = "DUPLICATE_BET";
            return;
          }

          const selected = new Set();

          for (const bet of Object.values(activeBets)) {
            if (bet.uid === uid) {
              selected.add(Number(bet.foodIndex));
            }
          }

          if (
            !selected.has(foodIndex) &&
            selected.size >= 6
          ) {
            rejection = "MAX_SIX_SLOTS";
            return;
          }

          activeBets[betKey] = {
            uid,
            betId,
            foodIndex,
            amount,
            roundNumber,
            createdAt: Date.now()
          };

          root.Users[uid].coins = coins - amount;

          root[GAME_PATH] = {
            ...current,
            activeBets
          };

          return root;
        },
        undefined,
        false
      );

    if (!tx.committed) {
      return {
        ok: false,
        error: rejection
      };
    }

    const savedRoot = tx.snapshot.val();

    if (
      savedRoot &&
      validGame(savedRoot[GAME_PATH])
    ) {
      game = savedRoot[GAME_PATH];
      lastSync = Date.now();
    }

    console.log(
      "BET ACCEPTED:",
      uid,
      amount,
      foodIndex
    );

    return {
      ok: true,
      betId,
      foodIndex,
      amount,
      roundNumber,
      balance: Number(
        savedRoot?.Users?.[uid]?.coins
      )
    };
  } catch (error) {
    console.error("BET ERROR:", error.message);

    return {
      ok: false,
      error: "BET_FAILED"
    };
  }
}

/*
  PAYOUT ENGINE

  Each winning user's total payout is credited
  using a transaction on Users/{uid}.

  lionPayouts/{roundNumber} records completed
  payouts inside that same transaction.

  Repeating a settlement therefore does not
  credit the same user twice.
*/

function calculatePayouts(entry) {
  const totals = {};

  const winner = Number(entry.winningIndex);

  for (const bet of Object.values(entry.bets || {})) {
    const uid = bet.uid;
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);

    if (
      typeof uid !== "string" ||
      !uid ||
      index !== winner ||
      !Number.isSafeInteger(amount) ||
      amount <= 0
    ) {
      continue;
    }

    const payout = amount * MULTIPLIERS[index];

    if (!Number.isSafeInteger(payout)) continue;

    totals[uid] = (totals[uid] || 0) + payout;
  }

  return totals;
}

async function creditWinner(uid, roundNumber, amount) {
  const ref = admin.database().ref(`Users/${uid}`);

  const key = String(roundNumber);

  const tx = await ref.transaction(
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) {
        return;
      }

      const coins = Number(user.coins);

      if (
        !Number.isSafeInteger(coins) ||
        coins < 0 ||
        !Number.isSafeInteger(amount) ||
        amount <= 0 ||
        !Number.isSafeInteger(coins + amount)
      ) {
        return;
      }

      return {
        ...user,
        coins: coins + amount,
        lionPayouts: {
          ...payouts,
          [key]: {
            amount,
            completedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  if (tx.committed) return true;

  const saved = tx.snapshot.val();

  return Boolean(
    saved &&
    saved.lionPayouts &&
    saved.lionPayouts[key]
  );
}

async function settlePending() {
  if (
    !firebaseReady ||
    settlementProcessing ||
    !game
  ) {
    return;
  }

  settlementProcessing = true;

  try {
    const snapshot = await gameRef()
      .child("pendingSettlement")
      .once("value");

    const pending = snapshot.val() || {};

    for (const [roundKey, entry] of Object.entries(pending)) {
      if (!entry || entry.settled) continue;

      // Do not automatically settle old records from
      // previous server versions.
      if (entry.payoutVersion !== 1) continue;

      const totals = calculatePayouts(entry);

      let allPaid = true;

      for (const [uid, amount] of Object.entries(totals)) {
        try {
          const paid = await creditWinner(
            uid,
            entry.roundNumber,
            amount
          );

          if (!paid) {
            allPaid = false;
            console.error(
              "PAYOUT PENDING:",
              uid,
              roundKey
            );
          }
        } catch (error) {
          allPaid = false;

          console.error(
            "PAYOUT ERROR:",
            error.message
          );
        }
      }

      if (!allPaid) continue;

      const settlementRef = gameRef()
        .child("pendingSettlement")
        .child(roundKey);

      await settlementRef.child("settled")
        .set(true);

      await settlementRef.child("settledAt")
        .set(Date.now());

      console.log("ROUND SETTLED:", roundKey);
    }
  } catch (error) {
    console.error(
      "SETTLEMENT ERROR:",
      error.message
    );
  } finally {
    settlementProcessing = false;
  }
}

/*
  TIMER

  Clock events are sent independently.

  The settlement engine runs separately.
*/

setInterval(() => {
  sendClock();

  if (!firebaseReady) return;

  if (!game) {
    if (!loadProcessing) {
      loadGame().catch(console.error);
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

  const needsSync = now - lastSync >= 10000;

  if (
    (needsUpdate || needsSync) &&
    !roundProcessing
  ) {
    advanceGame().catch(console.error);
  }
}, 1000);

setInterval(() => {
  if (!settlementProcessing) {
    settlePending().catch(console.error);
  }
}, 5000);

/*
  SOCKET.IO
*/

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
    async (data, ack) => {
      const reply =
        typeof ack === "function"
          ? ack
          : () => {};

      try {
        const result = await placeBet(data);

        reply(result);
      } catch (error) {
        console.error(
          "SOCKET BET ERROR:",
          error.message
        );

        reply({
          ok: false,
          error: "SERVER_ERROR"
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

/*
  HEALTH CHECK
*/

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

  console.log(
    "BETTING ENABLED:",
    BETTING_ENABLED
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
