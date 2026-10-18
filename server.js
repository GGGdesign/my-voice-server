
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

app.use(express.json({ limit: "16kb" }));

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

// Gift prices are provisional.
// Match these IDs and prices to the Android gift catalog.
const GIFT_CATALOG = Object.freeze({
  rose: { name: "Rose", price: 100 },
  heart: { name: "Heart", price: 200 },
  flower: { name: "Flower", price: 500 },
  cake: { name: "Cake", price: 1000 },
  car: { name: "Car", price: 5000 },
  crown: { name: "Crown", price: 10000 }
});

// Enable only after checking the real gift catalog
// and securing Firebase wallet rules.
const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS === "true";

let firebaseReady = false;
let game = null;
let roundProcessing = false;
let settlementProcessing = false;
let loading = false;
let lastSync = 0;

// =====================================================
// FIREBASE
// =====================================================

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

const db = () => admin.database();
const gameRef = () => db().ref(GAME_PATH);
const userRef = uid => db().ref(`Users/${uid}`);

// =====================================================
// AUTHENTICATION
// =====================================================

async function verifyRequestUser(req) {
  const authorization =
    req.headers.authorization || "";

  const match =
    /^Bearer (.+)$/i.exec(authorization);

  if (!match) {
    const error = new Error("TOKEN_REQUIRED");
    error.status = 401;
    throw error;
  }

  try {
    return await admin.auth().verifyIdToken(
      match[1],
      true
    );
  } catch {
    const error = new Error("INVALID_TOKEN");
    error.status = 401;
    throw error;
  }
}

// =====================================================
// GAME STATE
// =====================================================

function validGame(g) {
  return Boolean(
    g &&
    Number.isSafeInteger(g.roundNumber) &&
    g.roundNumber > 0 &&
    Number.isFinite(Number(g.roundStartsAt)) &&
    Number.isFinite(Number(g.roundEndsAt))
  );
}

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort((a, b) =>
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
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled: BETTING_ENABLED && firebaseReady,
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    multipliers: MULTIPLIERS,
    history: historyArray(game?.history)
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

  io.to(GAME_ROOM).emit("greedy_lion_clock", {
    roundNumber: game.roundNumber,
    roundStartsAt: game.roundStartsAt,
    roundEndsAt: game.roundEndsAt,
    serverTime: Date.now(),
    secondsRemaining: remaining()
  });
}

async function loadGame() {
  if (!firebaseReady || loading) return;

  loading = true;

  try {
    let saved = (await gameRef().once("value")).val();

    if (saved === null) {
      const now = Date.now();

      const tx = await gameRef().transaction(
        current => current === null
          ? createGame(now)
          : undefined,
        undefined,
        false
      );

      saved = tx.snapshot.val();

      if (!saved) {
        saved = (await gameRef().once("value")).val();
      }
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
    loading = false;
  }
}

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

        if (!validGame(current)) return;

        const start = Number(current.roundStartsAt);
        const end = Number(current.roundEndsAt);

        if (
          end - start !== ROUND_MS ||
          start > now + ROUND_MS
        ) {
          return {
            ...current,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_MS
          };
        }

        if (Date.now() < end) return;

        const roundNumber = current.roundNumber;
        const activeBets = current.activeBets || {};

        const pending = {
          ...(current.pendingSettlement || {})
        };

        const result = {
          roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        if (Object.keys(activeBets).length) {
          pending[String(roundNumber)] = {
            roundNumber,
            winningIndex: winner,
            completedAt: now,
            payoutVersion: 2,
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
          pendingSettlement: pending,
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

    const previous = game?.roundNumber;

    game = saved;
    lastSync = Date.now();

    if (
      previous != null &&
      game.roundNumber > previous
    ) {
      const completed = game.roundNumber - 1;

      const result = historyArray(game.history)
        .find(x => x.roundNumber === completed);

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log("ROUND FINISHED:", completed);
    }

    sendState();
    sendClock();
  } catch (error) {
    console.error("ROUND ERROR:", error.message);
  } finally {
    roundProcessing = false;
  }
}

// =====================================================
// GAME WALLET TRANSACTIONS
// =====================================================

async function reserveCoins(
  uid,
  betId,
  amount,
  roundNumber
) {
  const ref = userRef(uid);
  let reason = "WALLET_REJECTED";

  const tx = await ref.transaction(
    user => {
      if (!user) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const bets = user.lionBets || {};

      if (bets[betId]) {
        reason = "DUPLICATE_BET";
        return;
      }

      const coins = Number(user.coins);

      if (
        !Number.isSafeInteger(coins) ||
        coins < amount
      ) {
        reason = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...user,
        coins: coins - amount,
        lionBets: {
          ...bets,
          [betId]: {
            amount,
            roundNumber,
            status: "reserved",
            createdAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return {
    ok: tx.committed,
    error: tx.committed ? null : reason,
    balance: tx.snapshot.val()?.coins
  };
}

async function setBetStatus(uid, betId, status) {
  const ref = userRef(uid);

  const tx = await ref.transaction(
    user => {
      if (!user?.lionBets?.[betId]) return;

      const bet = user.lionBets[betId];

      if (bet.status === status) return;
      if (bet.status !== "reserved") return;

      return {
        ...user,
        lionBets: {
          ...user.lionBets,
          [betId]: {
            ...bet,
            status,
            updatedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]?.status === status;
}

async function refundBet(uid, betId) {
  const tx = await userRef(uid).transaction(
    user => {
      if (!user?.lionBets?.[betId]) return;

      const bet = user.lionBets[betId];

      if (bet.status === "refunded") return;
      if (bet.status !== "reserved") return;

      const coins = Number(user.coins);
      const amount = Number(bet.amount);

      if (
        !Number.isSafeInteger(coins) ||
        !Number.isSafeInteger(amount) ||
        !Number.isSafeInteger(coins + amount)
      ) {
        return;
      }

      return {
        ...user,
        coins: coins + amount,
        lionBets: {
          ...user.lionBets,
          [betId]: {
            ...bet,
            status: "refunded",
            refundedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]?.status ===
      "refunded";
}

async function placeBet(data) {
  if (!BETTING_ENABLED) {
    return {
      ok: false,
      error: "BETTING_DISABLED"
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
  } catch {
    return {
      ok: false,
      error: "INVALID_TOKEN"
    };
  }

  const uid = decoded.uid;
  const betKey = `${uid}_${betId}`;

  const currentGame =
    (await gameRef().once("value")).val();

  if (
    !validGame(currentGame) ||
    currentGame.roundNumber !== roundNumber ||
    Date.now() >= currentGame.roundEndsAt
  ) {
    return {
      ok: false,
      error: "ROUND_CLOSED"
    };
  }

  let reserved;

  try {
    reserved = await reserveCoins(
      uid,
      betId,
      amount,
      roundNumber
    );
  } catch (error) {
    console.error("RESERVE ERROR:", error.message);

    return {
      ok: false,
      error: "WALLET_ERROR"
    };
  }

  if (!reserved.ok) {
    return {
      ok: false,
      error: reserved.error
    };
  }

  let rejection = "BET_REJECTED";

  try {
    const tx = await gameRef().transaction(
      current => {
        if (!validGame(current)) {
          rejection = "GAME_NOT_READY";
          return;
        }

        if (
          current.roundNumber !== roundNumber ||
          Date.now() >= current.roundEndsAt
        ) {
          rejection = "ROUND_CLOSED";
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

        return {
          ...current,
          activeBets
        };
      },
      undefined,
      false
    );

    if (!tx.committed) {
      const refunded = await refundBet(uid, betId);

      return {
        ok: false,
        error: refunded
          ? rejection
          : "REFUND_PENDING"
      };
    }

    try {
      await setBetStatus(uid, betId, "accepted");
    } catch (error) {
      console.error(
        "BET STATUS ERROR:",
        error.message
      );
    }

    game = tx.snapshot.val();
    lastSync = Date.now();

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
      balance: reserved.balance
    };
  } catch (error) {
    console.error("BET ERROR:", error.message);

    try {
      const snap = await gameRef().once("value");
      const saved = snap.val();

      const inActive = Boolean(
        saved?.activeBets?.[betKey]
      );

      const inPending = Object.values(
        saved?.pendingSettlement || {}
      ).some(entry =>
        Boolean(entry?.bets?.[betKey])
      );

      if (inActive || inPending) {
        return {
          ok: true,
          betId,
          foodIndex,
          amount,
          roundNumber,
          balance: reserved.balance
        };
      }

      const refunded = await refundBet(uid, betId);

      return {
        ok: false,
        error: refunded
          ? "BET_FAILED_REFUNDED"
          : "BET_STATUS_UNCERTAIN"
      };
    } catch {
      return {
        ok: false,
        error: "BET_STATUS_UNCERTAIN"
      };
    }
  }
}

// =====================================================
// GAME PAYOUTS
// =====================================================

function calculatePayouts(entry) {
  const totals = {};
  const winner = Number(entry.winningIndex);

  for (const bet of Object.values(entry.bets || {})) {
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);
    const uid = bet.uid;

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
  const key = String(roundNumber);

  const tx = await userRef(uid).transaction(
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) return;

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

  return Boolean(
    tx.snapshot.val()?.lionPayouts?.[key]
  );
}

async function settlePending() {
  if (
    !firebaseReady ||
    !game ||
    settlementProcessing
  ) {
    return;
  }

  settlementProcessing = true;

  try {
    const snapshot = await gameRef()
      .child("pendingSettlement")
      .once("value");

    const pending = snapshot.val() || {};

    for (const [key, entry] of Object.entries(pending)) {
      if (!entry || entry.settled) continue;

      if (
        entry.payoutVersion !== 1 &&
        entry.payoutVersion !== 2
      ) {
        continue;
      }

      const totals = calculatePayouts(entry);
      let allPaid = true;

      for (const [uid, amount] of Object.entries(totals)) {
        try {
          const paid = await creditWinner(
            uid,
            entry.roundNumber,
            amount
          );

          if (!paid) allPaid = false;
        } catch (error) {
          allPaid = false;

          console.error(
            "PAYOUT ERROR:",
            error.message
          );
        }
      }

      if (!allPaid) continue;

      const ref = gameRef()
        .child("pendingSettlement")
        .child(key);

      await ref.update({
        settled: true,
        settledAt: Date.now()
      });

      console.log("ROUND SETTLED:", key);
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

// =====================================================
// SECURE GIFT SYSTEM
// =====================================================

function giftError(res, status, error) {
  return res.status(status).json({
    ok: false,
    error
  });
}

function validGiftId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(value);
}

function validGiftRequestId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{16,100}$/.test(value);
}

function sameGiftPayment(payment, request) {
  return Boolean(
    payment &&
    payment.roomId === request.roomId &&
    payment.giftId === request.giftId &&
    payment.receiverId === request.receiverId &&
    payment.quantity === request.quantity &&
    payment.amount === request.amount
  );
}

async function giftReceiverOnSeat(roomId, receiverId) {
  const snapshot = await db()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let found = false;

  snapshot.forEach(seat => {
    const id = String(
      seat.child("userId").val() || ""
    );

    if (id === receiverId) {
      found = true;
    }
  });

  return found;
}

async function chargeGift(uid, request) {
  let rejection = "PAYMENT_REJECTED";

  const tx = await userRef(uid).transaction(
    user => {
      if (!user) {
        rejection = "USER_NOT_FOUND";
        return;
      }

      const payments = user.giftPayments || {};
      const previous = payments[request.requestId];

      if (previous) {
        rejection = sameGiftPayment(previous, request)
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";

        return;
      }

      const coins = Number(user.coins);

      if (
        !Number.isSafeInteger(coins) ||
        coins < 0
      ) {
        rejection = "INVALID_WALLET";
        return;
      }

      if (coins < request.amount) {
        rejection = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...user,
        coins: coins - request.amount,
        giftPayments: {
          ...payments,
          [request.requestId]: {
            roomId: request.roomId,
            giftId: request.giftId,
            receiverId: request.receiverId,
            quantity: request.quantity,
            amount: request.amount,
            status: "paid",
            createdAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  const savedUser = tx.snapshot.val();

  const payment =
    savedUser?.giftPayments?.[request.requestId];

  if (!sameGiftPayment(payment, request)) {
    return {
      ok: false,
      error: rejection
    };
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: savedUser.coins,
    senderId: String(savedUser.customId || ""),
    senderName: String(savedUser.name || "مستخدم"),
    createdAt: payment.createdAt
  };
}

async function publishGift(uid, request, payment) {
  const eventId = `${uid}_${request.requestId}`;

  const event = {
    requestId: request.requestId,
    senderUid: uid,
    senderId: payment.senderId,
    senderName: payment.senderName,
    receiverId: request.receiverId,
    giftId: request.giftId,
    quantity: request.quantity,
    amount: request.amount,
    createdAt: payment.createdAt
  };

  const ref = db()
    .ref("rooms")
    .child(request.roomId)
    .child("giftEvents")
    .child(eventId);

  const tx = await ref.transaction(
    current => {
      if (current !== null) return;
      return event;
    },
    undefined,
    false
  );

  const saved = tx.snapshot.val();

  if (
    !saved ||
    saved.senderUid !== uid ||
    saved.requestId !== request.requestId ||
    saved.giftId !== request.giftId ||
    saved.receiverId !== request.receiverId ||
    saved.quantity !== request.quantity ||
    saved.amount !== request.amount
  ) {
    throw new Error("GIFT_EVENT_CONFLICT");
  }

  return saved;
}

// Gift catalog endpoint
app.get("/api/gifts/catalog", (req, res) => {
  return res.json({
    ok: true,
    enabled: GIFTS_ENABLED && firebaseReady,
    gifts: GIFT_CATALOG
  });
});

// Secure gift sending endpoint
app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady) {
    return giftError(
      res,
      503,
      "SERVER_NOT_READY"
    );
  }

  if (!GIFTS_ENABLED) {
    return giftError(
      res,
      503,
      "GIFTS_DISABLED"
    );
  }

  let decoded;

  try {
    decoded = await verifyRequestUser(req);
  } catch (error) {
    return giftError(
      res,
      error.status || 401,
      error.message
    );
  }

  const uid = decoded.uid;

  const roomId = String(req.body?.roomId || "");
  const giftId = String(req.body?.giftId || "");
  const receiverId = String(req.body?.receiverId || "");
  const requestId = String(req.body?.requestId || "");
  const quantity = req.body?.quantity;

  if (
    !validGiftId(roomId) ||
    !validGiftId(giftId) ||
    !/^[0-9]{1,20}$/.test(receiverId) ||
    !validGiftRequestId(requestId) ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 99
  ) {
    return giftError(
      res,
      400,
      "INVALID_GIFT_REQUEST"
    );
  }

  if (
    !Object.prototype.hasOwnProperty.call(
      GIFT_CATALOG,
      giftId
    )
  ) {
    return giftError(
      res,
      400,
      "UNKNOWN_GIFT"
    );
  }

  const amount =
    GIFT_CATALOG[giftId].price * quantity;

  if (
    !Number.isSafeInteger(amount) ||
    amount <= 0
  ) {
    return giftError(
      res,
      400,
      "INVALID_AMOUNT"
    );
  }

  const request = {
    roomId,
    giftId,
    receiverId,
    requestId,
    quantity,
    amount
  };

  try {
    // Repeated requests must retain the same details.
    const previousSnapshot = await userRef(uid)
      .child("giftPayments")
      .child(requestId)
      .once("value");

    const previous = previousSnapshot.val();

    if (previous) {
      if (!sameGiftPayment(previous, request)) {
        return giftError(
          res,
          409,
          "REQUEST_ID_CONFLICT"
        );
      }
    } else {
      const exists = await giftReceiverOnSeat(
        roomId,
        receiverId
      );

      if (!exists) {
        return giftError(
          res,
          404,
          "RECEIVER_NOT_ON_SEAT"
        );
      }
    }

    const payment = await chargeGift(
      uid,
      request
    );

    if (!payment.ok) {
      const status =
        payment.error === "INSUFFICIENT_COINS"
          ? 402
          : 409;

      return giftError(
        res,
        status,
        payment.error
      );
    }

    // The event is stored only after payment.
    // Retrying the same request cannot charge twice.
    await publishGift(
      uid,
      request,
      payment
    );

    return res.json({
      ok: true,
      requestId,
      roomId,
      giftId,
      receiverId,
      quantity,
      amount,
      balance: payment.balance,
      alreadyPaid: !payment.newlyCharged
    });
  } catch (error) {
    console.error(
      "GIFT SEND ERROR:",
      error.message
    );

    return res.status(503).json({
      ok: false,
      error: "GIFT_STATUS_UNCERTAIN",
      requestId
    });
  }
});

// =====================================================
// INDEPENDENT GAME CLOCK
// =====================================================

setInterval(() => {
  sendClock();

  if (!firebaseReady) return;

  if (!game) {
    if (!loading) loadGame().catch(console.error);
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

// =====================================================
// VOICE AND GAME SOCKET EVENTS
// =====================================================

io.on("connection", socket => {
  console.log("USER CONNECTED:", socket.id);

  socket.on("join_room", data => {
    if (!data?.roomId) return;

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
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

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

    if (!game) await loadGame();

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
        reply(await placeBet(data));
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
    console.log("USER DISCONNECTED:", socket.id);
  });
});

// =====================================================
// SERVER STATUS
// =====================================================

app.get("/", (req, res) => {
  const history = historyArray(game?.history);

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    giftsEnabled: GIFTS_ENABLED && firebaseReady,
    roundNumber: game?.roundNumber ?? null,
    secondsRemaining: remaining(),
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    durationMs: game
      ? Number(game.roundEndsAt) -
        Number(game.roundStartsAt)
      : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

// =====================================================
// SERVER STARTUP
// =====================================================

server.listen(PORT, async () => {
  console.log("MAX CHAT SERVER RUNNING:", PORT);
  console.log("BETTING ENABLED:", BETTING_ENABLED);
  console.log("GIFTS ENABLED:", GIFTS_ENABLED);

  await loadGame();

  if (game) await advanceGame();

  console.log(
    "GAME READY:",
    game?.roundNumber ?? "NOT READY"
  );
});
