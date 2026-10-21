"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "16kb" }));

const PORT = Number(process.env.PORT || 10000);
const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";
const SECRET_PATH = "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";
const ROUND_MS = 25000;
const RESULT_MS = 6000;
const CYCLE_MS = ROUND_MS + RESULT_MS;
const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];
const CHIPS = [100, 1000, 10000, 100000];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS !== "false";
const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

const GIFTS = Object.freeze({
  rose: { name: "وردة ملكية", price: 100 },
  heart: { name: "قلب الحب", price: 200 },
  bear: { name: "دبدوب", price: 500 },
  crown: { name: "تاج ملكي", price: 1000 },
  bouquet: { name: "باقة ورد", price: 2000 },
  car: { name: "سيارة فاخرة", price: 5000 },
  yacht: { name: "يخت", price: 10000 },
  palace: { name: "قصر", price: 20000 },
  diamond: { name: "ألماس", price: 50000 },
  horse: { name: "حصان ملكي", price: 100000 },
  dragon: { name: "تنين أسطوري", price: 500000 },
  galaxy: { name: "المجرة", price: 1000000 },
  tree: { name: "شجرة الذهب", price: 2000000 },
  throne: { name: "عرش ملكي", price: 5000000 },
  kingdom: { name: "مملكة الذهب", price: 10000000 }
});

let firebaseReady = false;

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

function db() {
  if (!firebaseReady) {
    throw new Error("FIREBASE_NOT_READY");
  }
  return admin.database();
}

const gameRef = () => db().ref(GAME_PATH);
const userRef = uid => db().ref("Users").child(uid);

// Verify that the record is visible before starting a parent-node transaction.
// Firebase RTDB may initially invoke a transaction updater with null while
// loading its local state. Never interpret that provisional null as proof that
// the authenticated user does not exist.
async function walletPreflight(uid) {
  const snapshot = await userRef(uid).once("value");
  if (!snapshot.exists()) {
    console.error("WALLET RECORD MISSING", {
      authProject: admin.app().options.credential?.projectId || "unknown",
      databaseURL: DB_URL,
      // Don't log names, emails, coins or tokens.
      uidSuffix: String(uid).slice(-5)
    });
    return errorResult("USER_NOT_FOUND");
  }
  const user = snapshot.val();
  if (!user || typeof user !== "object" ||
      !validCoins(Number(user.coins))) {
    return errorResult("INVALID_WALLET");
  }
  return { ok: true };
}


function validId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(value);
}

function validRequestId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{16,100}$/.test(value);
}

function validCoins(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function errorResult(error) {
  return { ok: false, error };
}

// A transaction already performs the required server-side read.  Installing
// an additional value listener on the whole user/game node before EVERY bet
// creates a redundant large Firebase download and slows acknowledgment.
async function transactionReady(ref, updater) {
  return ref.transaction(updater, undefined, false);
}

// Wallet-only compare-and-swap with Firebase REST ETags. Unlike the RTDB SDK
// transaction callback, REST GET returns the persisted value (not an initial
// provisional null). If a concurrent update wins, HTTP 412 makes us retry.
// This never creates a missing wallet and never updates another user's path.
async function walletTransactionReady(ref, updater) {
  const base = String(ref.toString());
  if (!base.startsWith(DB_URL + "/Users/")) {
    throw new Error("INVALID_WALLET_PATH");
  }
  const tokenData = await admin.app().options.credential.getAccessToken();
  if (!tokenData?.access_token) throw new Error("FIREBASE_ACCESS_TOKEN_MISSING");
  const url = base + ".json";
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const snapshot = value => ({ val: () => value });
  for (let attempt = 0; attempt < 8; attempt++) {
    const read = await fetch(url, {
      method: "GET",
      headers: { ...headers, "X-Firebase-ETag": "true" },
      signal: AbortSignal.timeout(12000)
    });
    if (!read.ok) throw new Error(`WALLET_READ_HTTP_${read.status}`);
    const etag = read.headers.get("etag");
    if (!etag) throw new Error("WALLET_ETAG_MISSING");
    const before = await read.json();
    // Return no-op only on real persisted null or an updater rejection.
    const after = updater(before);
    if (after === undefined) return { committed: false, snapshot: snapshot(before) };
    if (!after || typeof after !== "object") throw new Error("INVALID_WALLET_UPDATE");
    const write = await fetch(url, {
      method: "PUT",
      headers: { ...headers, "Content-Type": "application/json", "If-Match": etag },
      body: JSON.stringify(after),
      signal: AbortSignal.timeout(12000)
    });
    if (write.status === 412) continue;
    if (!write.ok) throw new Error(`WALLET_WRITE_HTTP_${write.status}`);
    return { committed: true, snapshot: snapshot(await write.json()) };
  }
  throw new Error("WALLET_CONCURRENT_RETRY_EXHAUSTED");
}

// Cache only already-verified ID tokens for a short period.  A fresh token
// still gets Firebase revocation verification; cache expires before token exp.
const verifiedTokens = new Map();
const VERIFY_CACHE_MS = 25000;
async function verifyToken(token) {
  if (!firebaseReady) throw new Error("SERVER_NOT_READY");
  if (!token || typeof token !== "string") throw new Error("TOKEN_REQUIRED");
  const key = crypto.createHash("sha256").update(token).digest("hex");
  const cached = verifiedTokens.get(key);
  if (cached && cached.expires > Date.now()) return cached.decoded;
  if (cached) verifiedTokens.delete(key);
  try {
    const decoded = await admin.auth().verifyIdToken(token, true);
    const expiry = Math.min(Date.now() + VERIFY_CACHE_MS,
      Number(decoded.exp || 0) * 1000 - 2000);
    if (expiry > Date.now()) verifiedTokens.set(key, { decoded, expires: expiry });
    if (verifiedTokens.size > 2000) {
      const now = Date.now();
      for (const [k, entry] of verifiedTokens) {
        if (entry.expires <= now || verifiedTokens.size > 1500) verifiedTokens.delete(k);
      }
    }
    return decoded;
  } catch {
    throw new Error("INVALID_TOKEN");
  }
}

async function verifyHttpUser(req) {
  const header = String(
    req.headers.authorization || ""
  );

  const match = /^Bearer\s+(.+)$/i.exec(header);

  if (!match) throw new Error("TOKEN_REQUIRED");

  return verifyToken(match[1]);
}

async function readWallet(uid, operation) {
  if (!validId(uid)) {
    return errorResult("INVALID_UID");
  }

  const snapshot = await userRef(uid).once("value");

  if (!snapshot.exists()) {
    console.error("USER NOT FOUND:", {
      operation,
      uid
    });
    return errorResult("USER_NOT_FOUND");
  }

  const user = snapshot.val();

  if (!user || typeof user !== "object") {
    return errorResult("INVALID_USER");
  }

  const coins = Number(user.coins);

  if (!validCoins(coins)) {
    return errorResult("INVALID_WALLET");
  }

  return { ok: true, uid, coins, user };
}

// ==================================================
// GAME STATE
// ==================================================

let game = null;
let gameStarted = false;
let roundProcessing = false;
let settlementProcessing = false;
// Requests received BEFORE the deadline must be finished before the draw.
// The client can display them optimistically; ONLY a server transaction
// changes coins, as required by the database security rules.
const incomingBets = new Map();
// Keep a small grace period for requests that reached the server before the
// cutoff; never stall the spin for 20 seconds when a Firebase request hangs.
const MAX_SETTLE_WAIT_MS = 12000;

function beginIncoming(round) {
  incomingBets.set(round, (incomingBets.get(round) || 0) + 1);
}
function endIncoming(round) {
  const count = (incomingBets.get(round) || 0) - 1;
  if (count <= 0) incomingBets.delete(round);
  else incomingBets.set(round, count);
}
function admissionFor(data) {
  if (!firebaseReady || !game) return errorResult("SERVER_NOT_READY");
  if (!BETTING_ENABLED) return errorResult("BETTING_DISABLED");
  const now = Date.now();
  const round = Number(data?.roundNumber);
  const index = Number(data?.foodIndex);
  const amount = Number(data?.amount);
  const id = String(data?.betId || "");
  if (!/^[a-f0-9]{32}$/i.test(id) || !Number.isInteger(index) ||
      index < 0 || index > 7 || !CHIPS.includes(amount) ||
      !Number.isSafeInteger(round)) return errorResult("INVALID_BET");
  if (game.roundNumber !== round || now >= Number(game.roundEndsAt))
    return errorResult("ROUND_CLOSED");
  // A player may repeat bets on chosen slots, but can choose no more than six.
  // The authenticated UID is checked within the authoritative transaction.
  return { ok: true, round, receivedAt: now };
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

function cycleEnd(value) {
  return Number(value?.cycleEndsAt || (Number(value?.roundEndsAt) + RESULT_MS));
}

function currentPhase(value, now = Date.now()) {
  if (!validGame(value)) return "waiting";
  if (now < Number(value.roundEndsAt)) return "betting";
  const recorded = historyArray(value.history).some(
    entry => Number(entry.roundNumber) === Number(value.roundNumber)
  );
  return recorded ? "result" : "closing";
}

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    cycleEndsAt: now + CYCLE_MS,
    activeBets: {},
    pendingSettlement: {},
    history: []
  };
}

function remaining() {
  if (!game) return null;
  const phase = currentPhase(game);
  if (phase === "closing") return 0;
  const until = phase === "betting"
    ? Number(game.roundEndsAt)
    : cycleEnd(game);
  return Math.max(0, Math.ceil((until - Date.now()) / 1000));
}

function publicState() {
  const phase = currentPhase(game);
  return {
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    cycleEndsAt: game ? cycleEnd(game) : null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled: firebaseReady && BETTING_ENABLED && phase === "betting",
    phase,
    multipliers: MULTIPLIERS,
    history: historyArray(game?.history)
  };
}

function sendState() {
  if (!game) return;
  io.to(GAME_ROOM).emit("greedy_lion_state", publicState());
}

function sendClock() {
  if (!game) return;
  const state = publicState();
  io.to(GAME_ROOM).emit("greedy_lion_clock", {
    roundNumber: state.roundNumber,
    roundStartsAt: state.roundStartsAt,
    roundEndsAt: state.roundEndsAt,
    cycleEndsAt: state.cycleEndsAt,
    serverTime: state.serverTime,
    secondsRemaining: state.secondsRemaining,
    bettingEnabled: state.bettingEnabled,
    phase: state.phase
  });
}

// ==================================================
// WINNER PROFILE
// ==================================================

function profileName(user) {
  return String(
    user?.name ||
    user?.userName ||
    user?.username ||
    user?.nickname ||
    user?.displayName ||
    user?.fullName ||
    user?.profile?.displayName ||
    user?.profile?.name ||
    "مستخدم"
  );
}

function profilePhoto(user) {
  // Some profiles have a flag in avatar but a real photo in another field.
  // Choose the first valid image URI, not simply the first non-empty field.
  const fields = [
    user?.profileImageUrl, user?.profileImage, user?.photoUrl,
    user?.photoURL, user?.avatarUrl, user?.imageUrl,
    user?.picture, user?.profilePhoto, user?.photo,
    user?.profile?.photoUrl, user?.profile?.photoURL
  ];
  for (const field of fields) {
    if (typeof field !== "string") continue;
    const value = field.trim();
    if (/^(https?:\/\/|data:image\/)/i.test(value)) return value;
  }
  return "";
}

function calculatePayouts(entry) {
  const totals = {};
  const winner = Number(entry.winningIndex);

  for (const bet of Object.values(entry.bets || {})) {
    const uid = bet.uid;
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);

    if (
      !validId(uid) ||
      index !== winner ||
      !validCoins(amount) ||
      amount === 0
    ) {
      continue;
    }

    const payout = amount * MULTIPLIERS[index];

    if (!validCoins(payout)) continue;

    const next = (totals[uid] || 0) + payout;

    if (!validCoins(next)) continue;

    totals[uid] = next;
  }

  return totals;
}

async function buildWinnerDetails(entry) {
  const totals = calculatePayouts(entry);
  const ordered = Object.entries(totals)
    .sort((a, b) => b[1] - a[1]);

  const winners = await Promise.all(
    ordered.map(async ([uid, amount], position) => {
      let user = {};

      try {
        const snapshot = await userRef(uid).once("value");
        user = snapshot.val() || {};
        // Prefer the actual Firebase Auth profile over non-image avatars.
        if (!profilePhoto(user) || profileName(user) === "مستخدم") {
          try {
            const authUser = await admin.auth().getUser(uid);
            user = {
              ...user,
              name: profileName(user) === "مستخدم"
                ? (authUser.displayName || "مستخدم") : profileName(user),
              profileImageUrl: profilePhoto(user) || authUser.photoURL || ""
            };
          } catch (authError) {
            console.warn("AUTH PROFILE UNAVAILABLE", uid, authError.message);
          }
        }
      } catch (error) {
        console.error(
          "WINNER PROFILE ERROR:",
          uid,
          error.message
        );
      }

      const name = profileName(user);
      const photoUrl = profilePhoto(user);

      return {
        rank: position + 1,
        uid,
        userUid: uid,
        userId: String(user.customId || user.userId || ""),
        name,
        userName: name,
        photoUrl,
        avatarUrl: photoUrl,
        amount,
        payout: amount,
        winnings: amount,
        winningIndex: Number(entry.winningIndex)
      };
    })
  );

  return winners;
}

async function publishRoundWinners(entry, preparedWinners = null) {
  const roundNumber = Number(entry.roundNumber);
  const winners = preparedWinners || await buildWinnerDetails(entry);

  const patch = {
    winners,
    topWinners: winners.slice(0, 3),
    hasSettledBets: true
  };

  // Existing history may be an array. Locate the result
  // through a transaction on the parent game state.
  const publishTransaction = await transactionReady(gameRef(), current => {
    if (!validGame(current)) return;

    const history = historyArray(current.history);
    const index = history.findIndex(
      item => Number(item.roundNumber) === roundNumber
    );

    if (index < 0) return;

    const previous = history[index];

    history[index] = {
      ...previous,
      ...patch
    };

    return {
      ...current,
      history
    };
  });

  const recorded = historyArray(publishTransaction.snapshot.val()?.history)
    .find(item => Number(item.roundNumber) === roundNumber);
  if (!recorded || recorded.hasSettledBets !== true) {
    throw new Error("WINNER_HISTORY_NOT_SAVED");
  }

  const result = {
    roundNumber,
    winningIndex: Number(entry.winningIndex),
    completedAt: Number(entry.completedAt),
    hasSettledBets: true,
    winners,
    topWinners: winners.slice(0, 3)
  };

  io.to(GAME_ROOM).emit(
    "greedy_lion_winners",
    result
  );

  io.to(GAME_ROOM).emit(
    "greedy_lion_round_result",
    result
  );

  return result;
}

// ==================================================
// GAME START
// ==================================================

async function startGame() {
  if (!firebaseReady || gameStarted) return;

  gameStarted = true;
  const ref = gameRef();

  try {
    const snapshot = await ref.once("value");

    if (!snapshot.exists()) {
      const now = Date.now();

      await transactionReady(ref, current => {
        if (current !== null) return;
        return createGame(now);
      });
    }

    ref.on(
      "value",
      snapshot => {
        const saved = snapshot.val();

        if (!validGame(saved)) {
          console.error("INVALID GAME STATE");
          return;
        }

        game = saved;
        sendState();
      },
      error => {
        console.error(
          "GAME LISTENER ERROR:",
          error.message
        );
      }
    );

    console.log("GAME LISTENER STARTED");
  } catch (error) {
    gameStarted = false;
    console.error("START GAME ERROR:", error.message);
  }
}

// ==================================================
// ROUND ADVANCE
// ==================================================

// Lock the winning index at the exact betting deadline. The *same*
// round stays visible during the six-second results window.
async function closeRound() {
  if (!firebaseReady || !game || roundProcessing) return;
  roundProcessing = true;
  try {
    const waiting = incomingBets.get(Number(game.roundNumber)) || 0;
    if (waiting > 0 && Date.now() < Number(game.roundEndsAt) + MAX_SETTLE_WAIT_MS) {
      // Protect all bets that arrived while the round was still open.
      return;
    }
    const win = crypto.randomInt(8);
    const now = Date.now();
    const tx = await transactionReady(gameRef(), current => {
      if (!validGame(current)) return;
      if (Date.now() < Number(current.roundEndsAt)) return;

      const roundNumber = Number(current.roundNumber);
      const history = historyArray(current.history);
      if (history.some(item => Number(item.roundNumber) === roundNumber)) return;
      const bets = current.activeBets || {};
      const pending = { ...(current.pendingSettlement || {}) };
      const result = {
        roundNumber,
        winningIndex: win,
        completedAt: now,
        hasSettledBets: Object.keys(bets).length === 0,
        winners: [],
        topWinners: []
      };
      if (Object.keys(bets).length > 0) {
        pending[String(roundNumber)] = {
          roundNumber, winningIndex: win, completedAt: now,
          payoutVersion: 2, settled: false, bets
        };
      }
      return {
        ...current,
        // Always give the result six full seconds even after slow bets.
        cycleEndsAt: Math.max(cycleEnd(current), Date.now() + RESULT_MS),
        pendingSettlement: pending,
        history: [...history, result].slice(-8)
      };
    });
    if (!tx.committed) return;
    game = tx.snapshot.val();
    const result = historyArray(game.history)
      .find(item => Number(item.roundNumber) === game.roundNumber);
    if (result) io.to(GAME_ROOM).emit("greedy_lion_round_result", result);
    sendState();
    sendClock();
    setImmediate(() => settlePending().catch(console.error));
  } catch (error) {
    console.error("CLOSE ROUND ERROR:", error.message);
  } finally {
    roundProcessing = false;
  }
}

// Clear only the old round's active-bet display once the RESULT window ends.
// Already-accepted bets remain in pendingSettlement for durable payouts.
async function nextRound() {
  if (!firebaseReady || !game || roundProcessing) return;
  roundProcessing = true;
  try {
    const now = Date.now();
    const tx = await transactionReady(gameRef(), current => {
      if (!validGame(current)) return;
      if (Date.now() < cycleEnd(current)) return;
      const r = Number(current.roundNumber);
      const recorded = historyArray(current.history).some(
        entry => Number(entry.roundNumber) === r
      );
      if (!recorded) return;
      return {
        ...current,
        roundNumber: r + 1,
        roundStartsAt: now,
        roundEndsAt: now + ROUND_MS,
        cycleEndsAt: now + CYCLE_MS,
        activeBets: {}
      };
    });
    if (!tx.committed) return;
    game = tx.snapshot.val();
    sendState();
    sendClock();
  } catch (error) {
    console.error("NEXT ROUND ERROR:", error.message);
  } finally {
    roundProcessing = false;
  }
}

// ==================================================
// BET RESERVATION
// ==================================================

async function reserveCoins(
  uid,
  betId,
  amount,
  roundNumber
) {
  // Unlimited repeated bets per selected slot, subject only to balance and
  // the six-DISTINCT-slots rule enforced in placeBet.
  // Prefetch once to distinguish a genuinely missing record from the
  // Firebase transaction updater's provisional null on an uncached node.
  // The actual debit and available-balance check remain atomic below.
  let reason = "WALLET_REJECTED";

  const tx = await walletTransactionReady(
    userRef(uid),
    user => {
      if (!user) {
        reason = "WALLET_TRANSACTION_EMPTY";
        return;
      }

      const bets = user.lionBets || {};

      if (bets[betId]) {
        reason = "DUPLICATE_BET";
        return;
      }

      const coins = Number(user.coins);

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (coins < amount) {
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
    }
  );

  if (!tx.committed) {
    return errorResult(reason);
  }

  return {
    ok: true,
    balance: Number(tx.snapshot.val()?.coins)
  };
}

async function setBetStatus(uid, betId, status) {
  const tx = await walletTransactionReady(
    userRef(uid),
    user => {
      const bet = user?.lionBets?.[betId];

      if (!bet) return;
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
    }
  );

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === status
  );
}

async function refundBet(uid, betId) {
  const tx = await walletTransactionReady(
    userRef(uid),
    user => {
      const bet = user?.lionBets?.[betId];

      if (!bet) return;
      if (bet.status === "refunded") return;
      if (bet.status !== "reserved") return;

      const amount = Number(bet.amount);
      const coins = Number(user.coins);

      if (
        !validCoins(amount) ||
        !validCoins(coins) ||
        !Number.isSafeInteger(coins + amount)
      ) return;

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
    }
  );

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === "refunded"
  );
}

async function placeBet(data, admission) {
  if (!BETTING_ENABLED) {
    return errorResult("BETTING_DISABLED");
  }

  if (!firebaseReady || !game) {
    return errorResult("SERVER_NOT_READY");
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
    return errorResult("INVALID_BET");
  }

  let decoded;

  try {
    decoded = await verifyToken(token);
  } catch (error) {
    return errorResult(error.message);
  }

  const uid = decoded.uid;
  const betKey = `${uid}_${betId}`;
  if (!admission?.ok || admission.round !== roundNumber) {
    return errorResult("ROUND_CLOSED");
  }

  // Admission was recorded on socket receipt, BEFORE slow token / wallet IO.
  // A valid pre-deadline request may finish just after the clock hits zero.
  const currentGame = game;
  if (!validGame(currentGame) || currentGame.roundNumber !== roundNumber ||
      historyArray(currentGame.history).some(
        entry => Number(entry.roundNumber) === roundNumber
      )) return errorResult("ROUND_CLOSED");
  // Fast rejection avoids a wallet debit/refund for an already-full board.
  const chosen = new Set(Object.values(currentGame.activeBets || {})
    .filter(b => b.uid === uid)
    .map(b => Number(b.foodIndex)));
  if (!chosen.has(foodIndex) && chosen.size >= 6)
    return errorResult("MAX_SIX_SLOTS");

  let reserved;

  try {
    reserved = await reserveCoins(
      uid,
      betId,
      amount,
      roundNumber
    );
  } catch (error) {
    console.error(
      "BET WALLET ERROR:",
      error.message
    );
    return errorResult("WALLET_ERROR");
  }

  if (!reserved.ok) return reserved;

  let reason = "BET_REJECTED";

  try {
    const tx = await transactionReady(
      gameRef(),
      current => {
        if (!validGame(current)) {
          reason = "GAME_NOT_READY";
          return;
        }

        if (
          current.roundNumber !== roundNumber ||
          historyArray(current.history).some(
            entry => Number(entry.roundNumber) === roundNumber
          )
        ) {
          reason = "ROUND_CLOSED";
          return;
        }

        const activeBets = {
          ...(current.activeBets || {})
        };

        if (activeBets[betKey]) {
          reason = "DUPLICATE_BET";
          return;
        }

        // Enforce six DISTINCT food slots atomically for concurrent bets.
        const occupied = new Set();
        for (const bet of Object.values(activeBets)) {
          if (bet.uid === uid) occupied.add(Number(bet.foodIndex));
        }
        if (!occupied.has(foodIndex) && occupied.size >= 6) {
          reason = "MAX_SIX_SLOTS";
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
      }
    );

    if (!tx.committed) {
      const refunded = await refundBet(uid, betId);

      return errorResult(
        refunded ? reason : "REFUND_PENDING"
      );
    }

    game = tx.snapshot.val();

    // The wager and wallet debit are now durable. Return the ACK immediately;
    // a third Firebase transaction merely annotates the accepted bet ledger.
    // Display confirms from the game commit ACK immediately. Status syncing
    // happens behind later queued bets, avoiding extra wallet CAS collisions.
    enqueueUserBet(uid, () => setBetStatus(uid, betId, "accepted"))
      .catch(error => console.error("BET STATUS ERROR:", error.message));

    return {
      ok: true,
      betId,
      foodIndex,
      amount,
      roundNumber,
      balance: reserved.balance
    };
  } catch (error) {
    console.error(
      "BET TRANSACTION ERROR:",
      error.message
    );

    try {
      const saved = (
        await gameRef().once("value")
      ).val();

      const inActive = Boolean(
        saved?.activeBets?.[betKey]
      );

      const inPending = Object.values(
        saved?.pendingSettlement || {}
      ).some(
        entry => Boolean(entry?.bets?.[betKey])
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

      return errorResult(
        refunded
          ? "BET_FAILED_REFUNDED"
          : "BET_STATUS_UNCERTAIN"
      );
    } catch {
      return errorResult("BET_STATUS_UNCERTAIN");
    }
  }
}

// ==================================================
// PAYOUTS
// ==================================================

async function creditWinner(
  uid,
  roundNumber,
  amount
) {
  const key = String(roundNumber);

  // Read the existing wallet once before its atomic payout transaction.
  // Idempotent credit: each uid/round has a single payout marker.
  const tx = await walletTransactionReady(
    userRef(uid),
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) return;

      const coins = Number(user.coins);

      if (
        !validCoins(coins) ||
        !validCoins(amount) ||
        amount === 0 ||
        !Number.isSafeInteger(coins + amount)
      ) return;

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
    }
  );

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionPayouts?.[key]
  );
}

async function settlePending() {
  if (
    !firebaseReady ||
    !game ||
    settlementProcessing
  ) return;

  settlementProcessing = true;

  try {
    const snapshot = await gameRef()
      .child("pendingSettlement")
      .once("value");

    const pending = snapshot.val() || {};

    for (const [key, entry] of Object.entries(pending)) {
      if (!entry) continue;

      if (
        entry.payoutVersion !== 1 &&
        entry.payoutVersion !== 2
      ) continue;

      let preparedWinners = null;
      let fullySettled = entry.settled === true;
      if (!entry.settled) {
        const totals = calculatePayouts(entry);
        // Profile loading and each player's independent wallet credit run
        // concurrently. Each credit is still idempotent per uid/round.
        // Credit wallets first; fetching profile photos must never delay winnings.
        const payments = await Promise.all(Object.entries(totals).map(
          async ([uid, amount]) => {
            try {
              return await creditWinner(uid, entry.roundNumber, amount);
            } catch (error) {
              console.error("PAYOUT ERROR:", uid, error.message);
              return false;
            }
          }
        ));
        if (payments.some(ok => !ok)) continue;
        // Tell connected clients as soon as the actual Firebase wallet credits
        // have succeeded. This is informational only: the wallet listener is
        // still the authoritative display of the new balance.
        io.to(GAME_ROOM).emit("greedy_lion_payouts_credited", {
          roundNumber: Number(entry.roundNumber),
          creditedAt: Date.now(),
          payouts: totals
        });
        await gameRef().child("pendingSettlement")
          .child(key).update({ settled: true, settledAt: Date.now() });
        fullySettled = true;
        // Winner names/photos are display-only and happen after durable credits.
        preparedWinners = await buildWinnerDetails(entry);
      }

      const hist = historyArray((await gameRef().child("history").once("value")).val());
      const published = hist.some(item =>
        Number(item.roundNumber) === Number(entry.roundNumber) &&
        item.hasSettledBets === true
      );
      if (!published) {
        try {
          await publishRoundWinners(entry, preparedWinners);
        } catch (error) {
          console.error("WINNER DISPLAY ERROR:", error.message);
        }
      }

      // Do not keep every past round's bet list inside greedyLionGame forever.
      // The old implementation accumulated all settled rounds, making EACH
      // new bet's game-root transaction progressively slower. The user payout
      // ledger and published history are already durable by this point.
      const latestHistory = historyArray(
        (await gameRef().child("history").once("value")).val()
      );
      const safelyPublished = latestHistory.some(item =>
        Number(item.roundNumber) === Number(entry.roundNumber) &&
        item.hasSettledBets === true
      );
      const beyondHistory = Number(entry.roundNumber) <
        Number(game?.roundNumber || 0) - 8;
      if (fullySettled && (safelyPublished || beyondHistory)) {
        await gameRef().child("pendingSettlement").child(key).remove();
      }

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

// ==================================================
// GIFTS
// ==================================================

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

async function giftReceiverOnSeat(
  roomId,
  receiverId
) {
  const snapshot = await db()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let found = false;

  snapshot.forEach(seat => {
    const userId = String(
      seat.child("userId").val() || ""
    );

    const userUid = String(
      seat.child("userUid").val() || ""
    );

    if (
      userId === receiverId ||
      userUid === receiverId
    ) {
      found = true;
    }
  });

  return found;
}

async function chargeGift(uid, request) {
  // /api/gifts/send already checked the sender and possible retry. The
  // transaction itself checks the actual balance against concurrent charges.
  let reason = "PAYMENT_REJECTED";

  const tx = await walletTransactionReady(
    userRef(uid),
    user => {
      if (!user) {
        reason = "WALLET_TRANSACTION_EMPTY";
        return;
      }

      const payments = user.giftPayments || {};
      const previous = payments[request.requestId];

      if (previous) {
        reason = sameGiftPayment(previous, request)
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";
        return;
      }

      const coins = Number(user.coins);

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (coins < request.amount) {
        reason = "INSUFFICIENT_COINS";
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
    }
  );

  const saved = tx.snapshot.val();
  const payment =
    saved?.giftPayments?.[request.requestId];

  if (!sameGiftPayment(payment, request)) {
    return errorResult(reason);
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: Number(saved.coins),
    senderId: String(saved.customId || ""),
    senderName: profileName(saved),
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

  const eventTx = await transactionReady(ref, current => {
    if (current !== null) return;
    return event;
  });

  const saved = eventTx.snapshot.val();

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

function giftError(res, status, error) {
  return res.status(status).json({
    ok: false,
    error
  });
}

app.get("/api/gifts/catalog", (req, res) => {
  res.json({
    ok: true,
    enabled: GIFTS_ENABLED && firebaseReady,
    gifts: GIFTS
  });
});

app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady) {
    return giftError(res, 503, "SERVER_NOT_READY");
  }

  if (!GIFTS_ENABLED) {
    return giftError(res, 503, "GIFTS_DISABLED");
  }

  let decoded;

  try {
    decoded = await verifyHttpUser(req);
  } catch (error) {
    return giftError(res, 401, error.message);
  }

  const uid = decoded.uid;
  const roomId = String(req.body?.roomId || "");
  const giftId = String(req.body?.giftId || "");
  const receiverId = String(req.body?.receiverId || "");
  const requestId = String(req.body?.requestId || "");
  const quantity = req.body?.quantity;

  if (
    !validId(roomId) ||
    !validId(giftId) ||
    !validId(receiverId) ||
    !validRequestId(requestId) ||
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

  const gift = GIFTS[giftId];

  if (!gift) {
    return giftError(res, 400, "UNKNOWN_GIFT");
  }

  const amount = gift.price * quantity;

  if (
    !validCoins(amount) ||
    amount === 0
  ) {
    return giftError(res, 400, "INVALID_AMOUNT");
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
    // No separate wallet read or seat-approval round trip.
    // Authentication, exact amount, balance and idempotency are enforced
    // atomically by chargeGift() below. Recipient is chosen by sender.

    const payment = await chargeGift(
      uid,
      request
    );

    if (!payment.ok) {
      return giftError(
        res,
        payment.error === "INSUFFICIENT_COINS"
          ? 402
          : 409,
        payment.error
      );
    }

    await publishGift(uid, request, payment);

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

// Sequentially commit a player's rapid clicks. No arbitrary click-count limit.
// Each click remains a separate bet with its own ID and exact amount.
const userBetTails = new Map();
function enqueueUserBet(uid, work) {
  const previous = userBetTails.get(uid) || Promise.resolve();
  const pending = previous.catch(() => {}).then(work);
  const tail = pending.catch(() => {});
  userBetTails.set(uid, tail);
  tail.finally(() => {
    if (userBetTails.get(uid) === tail) userBetTails.delete(uid);
  });
  return pending;
}

// Batch rapid clicks into one wallet reservation and one game commit.
// Every click keeps its unique betId; never issue a success ACK before both
// durable writes have completed. Per-user queue serializes against old clients.
async function placeBetBatch(uid, requests, admission) {
  if (!Array.isArray(requests) || requests.length === 0 || requests.length > 200)
    return errorResult("INVALID_BET_BATCH");
  const round = admission.round;
  const items = [];
  const ids = new Set();
  for (const value of requests) {
    const betId = String(value?.betId || "");
    const slot = Number(value?.foodIndex);
    const amount = Number(value?.amount);
    if (!/^[a-f0-9]{32}$/i.test(betId) || ids.has(betId) ||
        !Number.isInteger(slot) || slot < 0 || slot > 7 || !CHIPS.includes(amount))
      return errorResult("INVALID_BET_BATCH");
    ids.add(betId);
    items.push({ betId, foodIndex: slot, amount });
  }
  const existingGame = game;
  if (!validGame(existingGame) || existingGame.roundNumber !== round ||
      historyArray(existingGame.history).some(x => Number(x.roundNumber) === round))
    return errorResult("ROUND_CLOSED");
  const occupied = new Set(Object.values(existingGame.activeBets || {})
    .filter(x => x.uid === uid).map(x => Number(x.foodIndex)));
  items.forEach(x => occupied.add(x.foodIndex));
  if (occupied.size > 6) return errorResult("MAX_SIX_SLOTS");
  const sum = items.reduce((n, x) => n + x.amount, 0);
  if (!Number.isSafeInteger(sum)) return errorResult("INVALID_BET_BATCH");
  let reason = "WALLET_REJECTED";
  const reservation = await walletTransactionReady(userRef(uid), user => {
    if (!user) { reason = "USER_NOT_FOUND"; return; }
    const prior = user.lionBets || {};
    if (items.some(x => prior[x.betId])) { reason = "DUPLICATE_BET"; return; }
    const coins = Number(user.coins);
    if (!validCoins(coins)) { reason = "INVALID_WALLET"; return; }
    if (coins < sum) { reason = "INSUFFICIENT_COINS"; return; }
    const additions = {};
    for (const item of items) additions[item.betId] = {
      amount: item.amount, roundNumber: round, foodIndex: item.foodIndex,
      status: "reserved", createdAt: Date.now()
    };
    return { ...user, coins: coins - sum, lionBets: { ...prior, ...additions } };
  });
  if (!reservation.committed) return errorResult(reason);
  const refundAll = async () => {
    const tx = await walletTransactionReady(userRef(uid), user => {
      if (!user) return;
      const bets = { ...(user.lionBets || {}) };
      let refund = 0;
      for (const item of items) {
        if (bets[item.betId]?.status !== "reserved") continue;
        refund += item.amount;
        bets[item.betId] = { ...bets[item.betId], status: "refunded", refundedAt: Date.now() };
      }
      if (!refund) return;
      const coins = Number(user.coins);
      if (!validCoins(coins) || !Number.isSafeInteger(coins + refund)) return;
      return { ...user, coins: coins + refund, lionBets: bets };
    });
    return tx.committed || items.every(x => tx.snapshot.val()?.lionBets?.[x.betId]?.status === "refunded");
  };
  try {
    let rejection = "BET_REJECTED";
    const tx = await transactionReady(gameRef(), current => {
      if (!validGame(current) || current.roundNumber !== round ||
          historyArray(current.history).some(x => Number(x.roundNumber) === round)) {
        rejection = "ROUND_CLOSED"; return;
      }
      const active = { ...(current.activeBets || {}) };
      const selected = new Set(Object.values(active).filter(x => x.uid === uid)
        .map(x => Number(x.foodIndex)));
      items.forEach(x => selected.add(x.foodIndex));
      if (selected.size > 6) { rejection = "MAX_SIX_SLOTS"; return; }
      for (const item of items) {
        const key = `${uid}_${item.betId}`;
        if (active[key]) { rejection = "DUPLICATE_BET"; return; }
        active[key] = { uid, ...item, roundNumber: round, createdAt: admission.receivedAt };
      }
      return { ...current, activeBets: active };
    });
    if (!tx.committed) return errorResult((await refundAll()) ? rejection : "REFUND_PENDING");
    game = tx.snapshot.val();
    // One later status write for the entire batch, never block ACK on it.
    enqueueUserBet(uid, () => walletTransactionReady(userRef(uid), user => {
      if (!user) return;
      const bets = { ...(user.lionBets || {}) };
      let changed = false;
      for (const item of items) if (bets[item.betId]?.status === "reserved") {
        bets[item.betId] = { ...bets[item.betId], status: "accepted" };
        changed = true;
      }
      return changed ? { ...user, lionBets: bets } : undefined;
    })).catch(e => console.error("BATCH STATUS ERROR", e.message));
    return { ok: true, roundNumber: round, betIds: items.map(x => x.betId), balance: Number(reservation.snapshot.val()?.coins) };
  } catch (error) {
    console.error("BATCH COMMIT ERROR", error.message);
    // An uncertain game transaction must be checked before any refund.
    try {
      const stored = (await gameRef().once("value")).val();
      const persisted = items.every(x => Boolean(stored?.activeBets?.[`${uid}_${x.betId}`]) ||
        Object.values(stored?.pendingSettlement || {}).some(entry => Boolean(entry?.bets?.[`${uid}_${x.betId}`])));
      if (persisted) return { ok: true, roundNumber: round, betIds: items.map(x => x.betId) };
      const partial = items.some(x => Boolean(stored?.activeBets?.[`${uid}_${x.betId}`]) ||
        Object.values(stored?.pendingSettlement || {}).some(entry => Boolean(entry?.bets?.[`${uid}_${x.betId}`])));
      if (partial) return errorResult("BET_STATUS_UNCERTAIN");
      return errorResult((await refundAll()) ? "BET_FAILED_REFUNDED" : "REFUND_PENDING");
    } catch { return errorResult("BET_STATUS_UNCERTAIN"); }
  }
}

// ==================================================
// VOICE + SOCKET
// ==================================================

io.on("connection", socket => {
  console.log("USER CONNECTED:", socket.id);

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    socket.join(String(data.roomId));

    socket.to(String(data.roomId)).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_left_seat",
      { userId: data.userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_started_audio",
      { userId: data.userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_stopped_audio",
      { userId: data.userId }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
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
      await startGame();
    }

    if (!socket.connected || !game) return;

    socket.emit(
      "greedy_lion_state",
      publicState()
    );

    const state = publicState();
    socket.emit("greedy_lion_clock", {
      roundNumber: state.roundNumber,
      roundStartsAt: state.roundStartsAt,
      roundEndsAt: state.roundEndsAt,
      cycleEndsAt: state.cycleEndsAt,
      serverTime: state.serverTime,
      secondsRemaining: state.secondsRemaining,
      bettingEnabled: state.bettingEnabled,
      phase: state.phase
    });
  });

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const reply = typeof ack === "function" ? ack : () => {};
      const admission = admissionFor(data);
      if (!admission.ok) {
        reply(admission);
        return;
      }
      beginIncoming(admission.round);
      try {
        // Firebase wallet reads/writes for the same account must not race.
        // Validate account identity before queuing. The original reception time
        // was already captured in admissionFor().
        const decoded = await verifyToken(String(data?.idToken || ""));
        const result = await enqueueUserBet(decoded.uid, () => placeBet(data, admission));
        reply(result);
      } catch (error) {
        console.error("SOCKET BET ERROR:", error.message);
        reply(errorResult(error.message === "INVALID_TOKEN" ? "INVALID_TOKEN" : "BET_STATUS_UNCERTAIN"));
      } finally {
        endIncoming(admission.round);
      }
    }
  );

  socket.on("greedy_lion_place_batch", async (data, ack) => {
    const reply = typeof ack === "function" ? ack : () => {};
    const items = data?.bets;
    const first = Array.isArray(items) && items.length ? items[0] : null;
    const admission = admissionFor({ ...first, roundNumber: data?.roundNumber });
    if (!admission.ok) { reply(admission); return; }
    if (items.length > 200) { reply(errorResult("INVALID_BET_BATCH")); return; }
    beginIncoming(admission.round);
    try {
      const user = await verifyToken(String(data?.idToken || ""));
      const result = await enqueueUserBet(user.uid, () => placeBetBatch(user.uid, items, admission));
      reply(result);
    } catch (e) {
      console.error("BATCH BET ERROR", e.message);
      reply(errorResult(e.message === "INVALID_TOKEN" ? "INVALID_TOKEN" : "BET_STATUS_UNCERTAIN"));
    } finally { endIncoming(admission.round); }
  });

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log("USER DISCONNECTED:", socket.id);
  });
});

// ==================================================
// TIMERS
// ==================================================

let timerBusy = false;
setInterval(async () => {
  if (!firebaseReady) return;
  if (!game) {
    startGame().catch(console.error);
    return;
  }
  sendClock();
  if (timerBusy) return;
  timerBusy = true;
  try {
    const roundNumber = Number(game.roundNumber);
    const recorded = historyArray(game.history).some(
      item => Number(item.roundNumber) === roundNumber
    );
    if (Date.now() >= Number(game.roundEndsAt) && !recorded) {
      await closeRound();
    }
    if (Date.now() >= cycleEnd(game) && historyArray(game.history).some(
      item => Number(item.roundNumber) === Number(game.roundNumber)
    )) {
      await nextRound();
    }
  } catch (error) {
    console.error("TIMER ERROR:", error.message);
  } finally {
    timerBusy = false;
  }
}, 500);

// Faster fallback for settlement failures.
setInterval(() => {
  settlePending().catch(console.error);
}, 1000);

// Authenticated wallet diagnostic. Does not change credits or account identity.
app.get("/api/account/status", async (req, res) => {
  try {
    const decoded = await verifyHttpUser(req);
    const snapshot = await userRef(decoded.uid).once("value");
    const data = snapshot.val();
    if (!data || typeof data !== "object") {
      return res.status(404).json({
        ok: false, error: "AUTH_UID_HAS_NO_USERS_RECORD",
        hint: "Check Users/<FirebaseAuth.uid>, not the public customId",
        authenticatedUid: decoded.uid
      });
    }
    return res.json({
      ok: true, authenticatedUid: decoded.uid,
      publicId: String(data.customId || data.userId || ""),
      walletValid: validCoins(Number(data.coins)),
      accountFound: true
    });
  } catch (error) {
    return res.status(401).json({ ok: false, error: error.message });
  }
});

// ==================================================
// HEALTH
// ==================================================

app.get("/", (req, res) => {
  const history = historyArray(game?.history);

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled:
      firebaseReady && BETTING_ENABLED && currentPhase(game) === "betting",
    phase: currentPhase(game),
    cycleEndsAt: game ? cycleEnd(game) : null,
    giftsEnabled:
      firebaseReady && GIFTS_ENABLED,
    roundNumber: game?.roundNumber ?? null,
    secondsRemaining: remaining(),
    roundStartsAt:
      game?.roundStartsAt ?? null,
    roundEndsAt:
      game?.roundEndsAt ?? null,
    durationMs: ROUND_MS,
    inFlightBets: incomingBets.get(Number(game?.roundNumber)) || 0,
    resultDurationMs: RESULT_MS,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

// ==================================================
// START
// ==================================================

httpServer.listen(PORT, async () => {
  console.log("MAX CHAT SERVER RUNNING:", PORT);
  console.log("BETTING ENABLED:", BETTING_ENABLED);
  console.log("GIFTS ENABLED:", GIFTS_ENABLED);
  console.log("FIREBASE DATABASE URL:", DB_URL);
  console.log("FIREBASE APP PROJECT:", admin.app().options.projectId || "see-service-account");

  await startGame();

  settlePending().catch(console.error);

  console.log(
    "GAME READY:",
    game?.roundNumber ?? "NOT READY"
  );
});
