
"use strict";

function attachGreedyLion({
  app,
  io,
  admin,
  firebaseReady
}) {
  const crypto = require("crypto");

  const GAME_PATH = "greedyLionGame";
  const ROOM = "maxchat_greedy_lion_global";

  const BET_MS = 25000;
  const RESULT_MS = 6000;

  const MULTIPLIERS = [
    45, 5, 5, 10, 15, 5, 5, 25
  ];

  const CHIPS = [
    100, 500, 1000, 5000, 10000
  ];

  const BETS_ENABLED =
    process.env.LION_ENABLE_BETS !== "false";

  let ready = Boolean(firebaseReady);
  let game = null;
  let gameStarting = false;
  let advancing = false;
  let settling = false;

  function db() {
    if (!ready) {
      throw Error("FIREBASE_NOT_READY");
    }

    return admin.database();
  }

  function gameRef() {
    return db().ref(GAME_PATH);
  }

  function userRef(uid) {
    return db()
      .ref("Users")
      .child(uid);
  }

  function validId(value) {
    return typeof value === "string" &&
      /^[a-zA-Z0-9_-]{1,100}$/.test(value);
  }

  function validCoins(value) {
    return Number.isSafeInteger(value) &&
      value >= 0;
  }

  function fail(error) {
    return {
      ok: false,
      error
    };
  }

  async function transaction(ref, update) {
    let listener;

    try {
      await new Promise((resolve, reject) => {
        listener = resolve;

        ref.on(
          "value",
          listener,
          reject
        );
      });

      return await ref.transaction(
        update,
        undefined,
        false
      );
    } finally {
      if (listener) {
        ref.off("value", listener);
      }
    }
  }

  async function verifyToken(token) {
    if (!ready) {
      throw Error("SERVER_NOT_READY");
    }

    if (!token) {
      throw Error("TOKEN_REQUIRED");
    }

    try {
      return await admin.auth().verifyIdToken(
        String(token),
        true
      );
    } catch {
      throw Error("INVALID_TOKEN");
    }
  }

  // =====================================
  // GAME STATE
  // =====================================

  function historyArray(value) {
    if (Array.isArray(value)) {
      return value
        .filter(Boolean)
        .slice(-8);
    }

    if (value && typeof value === "object") {
      return Object.values(value)
        .filter(Boolean)
        .sort(
          (a, b) =>
            Number(a.roundNumber) -
            Number(b.roundNumber)
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
      Number.isFinite(
        Number(value.roundEndsAt)
      )
    );
  }

  function cycleEnd(value) {
    return Number(
      value.cycleEndsAt ||
      Number(value.roundEndsAt) + RESULT_MS
    );
  }

  function phase(value) {
    if (!validGame(value)) {
      return "waiting";
    }

    return Date.now() <
      Number(value.roundEndsAt)
      ? "betting"
      : "result";
  }

  function createGame(now) {
    return {
      roundNumber: 1,
      roundStartsAt: now,
      roundEndsAt: now + BET_MS,
      cycleEndsAt:
        now + BET_MS + RESULT_MS,
      activeBets: {},
      pendingSettlement: {},
      history: []
    };
  }

  function remaining() {
    if (!game) {
      return 0;
    }

    const end =
      phase(game) === "betting"
        ? Number(game.roundEndsAt)
        : cycleEnd(game);

    return Math.max(
      0,
      Math.ceil(
        (end - Date.now()) / 1000
      )
    );
  }

  function state() {
    return {
      roundNumber:
        game?.roundNumber || null,
      roundStartsAt:
        game?.roundStartsAt || null,
      roundEndsAt:
        game?.roundEndsAt || null,
      cycleEndsAt:
        game ? cycleEnd(game) : null,
      serverTime: Date.now(),
      secondsRemaining: remaining(),
      bettingEnabled:
        ready &&
        BETS_ENABLED &&
        phase(game) === "betting",
      phase: phase(game),
      multipliers: MULTIPLIERS,
      history: historyArray(
        game?.history
      )
    };
  }

  function sendState() {
    if (!game) {
      return;
    }

    io.to(ROOM).emit(
      "greedy_lion_state",
      state()
    );
  }

  function sendClock() {
    if (!game) {
      return;
    }

    const s = state();

    io.to(ROOM).emit(
      "greedy_lion_clock",
      {
        roundNumber: s.roundNumber,
        roundStartsAt: s.roundStartsAt,
        roundEndsAt: s.roundEndsAt,
        cycleEndsAt: s.cycleEndsAt,
        serverTime: s.serverTime,
        secondsRemaining:
          s.secondsRemaining,
        bettingEnabled:
          s.bettingEnabled,
        phase: s.phase
      }
    );
  }

  async function startGame() {
    if (!ready || gameStarting) {
      return;
    }

    gameStarting = true;

    try {
      const ref = gameRef();

      const snap =
        await ref.once("value");

      if (!snap.exists()) {
        await transaction(ref, current => {
          if (current !== null) {
            return;
          }

          return createGame(Date.now());
        });
      }

      ref.on("value", snapshot => {
        const saved = snapshot.val();

        if (!validGame(saved)) {
          return;
        }

        game = saved;
        sendState();
      });

      console.log(
        "LION GAME STARTED"
      );
    } catch (e) {
      gameStarting = false;

      console.error(
        "START ERROR:",
        e.message
      );
    }
  }

  // =====================================
  // WINNER PROFILES
  // =====================================

  function profileName(user) {
    return String(
      user?.name ||
      user?.userName ||
      user?.username ||
      user?.nickname ||
      "مستخدم"
    );
  }

  function profilePhoto(user) {
    return String(
      user?.profileImageUrl ||
      user?.profileImage ||
      user?.photoUrl ||
      user?.photoURL ||
      user?.avatarUrl ||
      user?.avatar ||
      user?.imageUrl ||
      ""
    );
  }

  function payouts(entry) {
    const totals = {};

    const index = Number(
      entry.winningIndex
    );

    for (
      const bet of Object.values(
        entry.bets || {}
      )
    ) {
      const uid = String(
        bet.uid || ""
      );

      const amount = Number(
        bet.amount
      );

      const slot = Number(
        bet.foodIndex
      );

      if (
        !validId(uid) ||
        slot !== index ||
        !validCoins(amount) ||
        amount === 0
      ) {
        continue;
      }

      const value =
        amount * MULTIPLIERS[index];

      const total =
        (totals[uid] || 0) + value;

      if (validCoins(total)) {
        totals[uid] = total;
      }
    }

    return totals;
  }

  async function winnerDetails(entry) {
    const ordered =
      Object.entries(
        payouts(entry)
      ).sort(
        (a, b) => b[1] - a[1]
      );

    return Promise.all(
      ordered.map(
        async ([uid, amount], i) => {
          let user = {};

          try {
            user = (
              await userRef(uid)
                .once("value")
            ).val() || {};
          } catch (e) {
            console.error(
              "PROFILE ERROR:",
              uid,
              e.message
            );
          }

          const name =
            profileName(user);

          const photoUrl =
            profilePhoto(user);

          return {
            rank: i + 1,
            uid,
            userUid: uid,
            userId: String(
              user.customId ||
              user.userId ||
              ""
            ),
            name,
            userName: name,
            photoUrl,
            avatarUrl: photoUrl,
            amount,
            payout: amount,
            winnings: amount,
            winningIndex: Number(
              entry.winningIndex
            )
          };
        }
      )
    );
  }

  async function publishWinners(entry) {
    const roundNumber = Number(
      entry.roundNumber
    );

    const winners =
      await winnerDetails(entry);

    const result = {
      roundNumber,
      winningIndex: Number(
        entry.winningIndex
      ),
      completedAt: Number(
        entry.completedAt
      ),
      hasSettledBets: true,
      winners,
      topWinners:
        winners.slice(0, 3)
    };

    const tx = await transaction(
      gameRef(),
      current => {
        if (!validGame(current)) {
          return;
        }

        const history =
          historyArray(
            current.history
          );

        const i =
          history.findIndex(
            x =>
              Number(
                x.roundNumber
              ) === roundNumber
          );

        if (i < 0) {
          return;
        }

        history[i] = {
          ...history[i],
          ...result
        };

        return {
          ...current,
          history
        };
      }
    );

    const saved = historyArray(
      tx.snapshot.val()?.history
    ).find(
      x =>
        Number(x.roundNumber) ===
        roundNumber
    );

    if (!saved?.hasSettledBets) {
      throw Error(
        "RESULT_NOT_SAVED"
      );
    }

    io.to(ROOM).emit(
      "greedy_lion_winners",
      result
    );

    io.to(ROOM).emit(
      "greedy_lion_round_result",
      result
    );
  }

  // =====================================
  // ROUND RESULT
  // =====================================

  async function announceResult() {
    if (
      !ready ||
      !game ||
      advancing
    ) {
      return;
    }

    advancing = true;

    try {
      const round = Number(
        game.roundNumber
      );

      const winner =
        crypto.randomInt(8);

      const now = Date.now();

      const tx = await transaction(
        gameRef(),
        current => {
          if (!validGame(current)) {
            return;
          }

          if (
            Number(
              current.roundNumber
            ) !== round ||
            Date.now() <
              Number(
                current.roundEndsAt
              )
          ) {
            return;
          }

          const history =
            historyArray(
              current.history
            );

          if (
            history.some(
              x =>
                Number(
                  x.roundNumber
                ) === round
            )
          ) {
            return;
          }

          const bets =
            current.activeBets || {};

          const pending = {
            ...(current.pendingSettlement || {})
          };

          if (
            Object.keys(bets).length
          ) {
            pending[
              String(round)
            ] = {
              roundNumber: round,
              winningIndex: winner,
              completedAt: now,
              payoutVersion: 2,
              settled: false,
              bets
            };
          }

          const result = {
            roundNumber: round,
            winningIndex: winner,
            completedAt: now,
            hasSettledBets:
              Object.keys(bets)
                .length === 0,
            winners: [],
            topWinners: []
          };

          return {
            ...current,
            pendingSettlement: pending,
            history: [
              ...history,
              result
            ].slice(-8)
          };
        }
      );

      if (!tx.committed) {
        return;
      }

      game = tx.snapshot.val();

      const result =
        historyArray(
          game.history
        ).find(
          x =>
            Number(
              x.roundNumber
            ) === round
        );

      if (result) {
        io.to(ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      sendState();

      settlePending().catch(
        console.error
      );
    } catch (e) {
      console.error(
        "RESULT ERROR:",
        e.message
      );
    } finally {
      advancing = false;
    }
  }

  // =====================================
  // NEXT ROUND
  // =====================================

  async function nextRound() {
    if (
      !ready ||
      !game ||
      advancing
    ) {
      return;
    }

    advancing = true;

    try {
      const now = Date.now();

      const tx = await transaction(
        gameRef(),
        current => {
          if (!validGame(current)) {
            return;
          }

          if (
            Date.now() <
            cycleEnd(current)
          ) {
            return;
          }

          const round = Number(
            current.roundNumber
          );

          const recorded =
            historyArray(
              current.history
            ).some(
              x =>
                Number(
                  x.roundNumber
                ) === round
            );

          if (!recorded) {
            return;
          }

          return {
            ...current,
            roundNumber:
              round + 1,
            roundStartsAt: now,
            roundEndsAt:
              now + BET_MS,
            cycleEndsAt:
              now +
              BET_MS +
              RESULT_MS,
            activeBets: {}
          };
        }
      );

      if (tx.committed) {
        game = tx.snapshot.val();

        sendState();
        sendClock();
      }
    } catch (e) {
      console.error(
        "NEXT ROUND ERROR:",
        e.message
      );
    } finally {
      advancing = false;
    }
  }

  // =====================================
  // BET WALLET
  // =====================================

  async function reserveBet(
    uid,
    betId,
    amount,
    round
  ) {
    let reason =
      "BET_REJECTED";

    const tx = await transaction(
      userRef(uid),
      user => {
        if (!user) {
          reason = "USER_NOT_FOUND";
          return;
        }

        const bets =
          user.lionBets || {};

        if (bets[betId]) {
          reason = "DUPLICATE_BET";
          return;
        }

        const coins =
          Number(user.coins);

        if (!validCoins(coins)) {
          reason = "INVALID_WALLET";
          return;
        }

        if (coins < amount) {
          reason =
            "INSUFFICIENT_COINS";
          return;
        }

        return {
          ...user,
          coins: coins - amount,
          lionBets: {
            ...bets,
            [betId]: {
              amount,
              roundNumber: round,
              status: "reserved",
              createdAt: Date.now()
            }
          }
        };
      }
    );

    if (!tx.committed) {
      return fail(reason);
    }

    return {
      ok: true,
      balance: Number(
        tx.snapshot.val().coins
      )
    };
  }

  async function updateBetStatus(
    uid,
    betId
  ) {
    await transaction(
      userRef(uid),
      user => {
        const bet =
          user?.lionBets?.[betId];

        if (
          !bet ||
          bet.status !== "reserved"
        ) {
          return;
        }

        return {
          ...user,
          lionBets: {
            ...user.lionBets,
            [betId]: {
              ...bet,
              status: "accepted"
            }
          }
        };
      }
    );
  }

  async function refundBet(
    uid,
    betId
  ) {
    const tx = await transaction(
      userRef(uid),
      user => {
        const bet =
          user?.lionBets?.[betId];

        if (
          !bet ||
          bet.status !== "reserved"
        ) {
          return;
        }

        const coins =
          Number(user.coins);

        const amount =
          Number(bet.amount);

        if (
          !validCoins(coins) ||
          !validCoins(amount) ||
          !validCoins(
            coins + amount
          )
        ) {
          return;
        }

        return {
          ...user,
          coins:
            coins + amount,
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
      tx.snapshot.val()
        ?.lionBets?.[betId]
        ?.status === "refunded"
    );
  }

  // =====================================
  // PLACE BET
  // =====================================

  async function placeBet(data) {
    if (!ready || !game) {
      return fail(
        "SERVER_NOT_READY"
      );
    }

    if (!BETS_ENABLED) {
      return fail(
        "BETTING_DISABLED"
      );
    }

    const token = String(
      data?.idToken || ""
    );

    const betId = String(
      data?.betId || ""
    );

    const foodIndex = Number(
      data?.foodIndex
    );

    const amount = Number(
      data?.amount
    );

    const round = Number(
      data?.roundNumber
    );

    if (
      !/^[a-f0-9]{32}$/i.test(betId) ||
      !Number.isInteger(foodIndex) ||
      foodIndex < 0 ||
      foodIndex > 7 ||
      !CHIPS.includes(amount) ||
      !Number.isSafeInteger(round)
    ) {
      return fail("INVALID_BET");
    }

    let uid;

    try {
      uid = (
        await verifyToken(token)
      ).uid;
    } catch (e) {
      return fail(e.message);
    }

    const key =
      `${uid}_${betId}`;

    const current = (
      await gameRef()
        .once("value")
    ).val();

    if (
      !validGame(current) ||
      Number(
        current.roundNumber
      ) !== round ||
      phase(current) !== "betting"
    ) {
      return fail(
        "ROUND_CLOSED"
      );
    }

    let reserved;

    try {
      reserved =
        await reserveBet(
          uid,
          betId,
          amount,
          round
        );
    } catch (e) {
      console.error(
        "RESERVE ERROR:",
        e.message
      );

      return fail(
        "WALLET_ERROR"
      );
    }

    if (!reserved.ok) {
      return reserved;
    }

    let reason =
      "BET_REJECTED";

    try {
      const tx = await transaction(
        gameRef(),
        current => {
          if (!validGame(current)) {
            reason =
              "GAME_NOT_READY";
            return;
          }

          if (
            Number(
              current.roundNumber
            ) !== round ||
            phase(current) !==
              "betting"
          ) {
            reason =
              "ROUND_CLOSED";
            return;
          }

          const activeBets = {
            ...(current.activeBets || {})
          };

          if (activeBets[key]) {
            reason =
              "DUPLICATE_BET";
            return;
          }

          const selected =
            new Set();

          for (
            const bet of
            Object.values(activeBets)
          ) {
            if (bet.uid === uid) {
              selected.add(
                Number(
                  bet.foodIndex
                )
              );
            }
          }

          if (
            !selected.has(foodIndex) &&
            selected.size >= 6
          ) {
            reason =
              "MAX_SIX_SLOTS";
            return;
          }

          activeBets[key] = {
            uid,
            betId,
            foodIndex,
            amount,
            roundNumber: round,
            createdAt: Date.now()
          };

          return {
            ...current,
            activeBets
          };
        }
      );

      if (!tx.committed) {
        const refunded =
          await refundBet(
            uid,
            betId
          );

        return fail(
          refunded
            ? reason
            : "REFUND_PENDING"
        );
      }

      game = tx.snapshot.val();

      try {
        await updateBetStatus(
          uid,
          betId
        );
      } catch (e) {
        console.error(
          "BET STATUS ERROR:",
          e.message
        );
      }

      return {
        ok: true,
        betId,
        foodIndex,
        amount,
        roundNumber: round,
        balance:
          reserved.balance
      };
    } catch (e) {
      console.error(
        "BET ERROR:",
        e.message
      );

      try {
        const saved = (
          await gameRef()
            .once("value")
        ).val();

        const active = Boolean(
          saved?.activeBets?.[key]
        );

        const pending =
          Object.values(
            saved?.pendingSettlement || {}
          ).some(
            entry =>
              Boolean(
                entry?.bets?.[key]
              )
          );

        if (
          active ||
          pending
        ) {
          return {
            ok: true,
            betId,
            foodIndex,
            amount,
            roundNumber: round,
            balance:
              reserved.balance
          };
        }

        const refunded =
          await refundBet(
            uid,
            betId
          );

        return fail(
          refunded
            ? "BET_FAILED_REFUNDED"
            : "BET_STATUS_UNCERTAIN"
        );
      } catch {
        return fail(
          "BET_STATUS_UNCERTAIN"
        );
      }
    }
  }

  // =====================================
  // WINNER PAYOUT
  // =====================================

  async function payWinner(
    uid,
    round,
    amount
  ) {
    const key =
      String(round);

    const tx = await transaction(
      userRef(uid),
      user => {
        if (!user) {
          return;
        }

        const paid =
          user.lionPayouts || {};

        if (paid[key]) {
          return;
        }

        const coins =
          Number(user.coins);

        if (
          !validCoins(coins) ||
          !validCoins(amount) ||
          amount === 0 ||
          !validCoins(
            coins + amount
          )
        ) {
          return;
        }

        return {
          ...user,
          coins:
            coins + amount,
          lionPayouts: {
            ...paid,
            [key]: {
              amount,
              completedAt:
                Date.now()
            }
          }
        };
      }
    );

    return Boolean(
      tx.committed ||
      tx.snapshot.val()
        ?.lionPayouts?.[key]
    );
  }

  // =====================================
  // SETTLEMENT
  // =====================================

  async function settlePending() {
    if (
      !ready ||
      !game ||
      settling
    ) {
      return;
    }

    settling = true;

    try {
      const pending = (
        await gameRef()
          .child(
            "pendingSettlement"
          )
          .once("value")
      ).val() || {};

      for (
        const [key, entry] of
        Object.entries(pending)
      ) {
        if (!entry) {
          continue;
        }

        if (
          entry.payoutVersion !== 1 &&
          entry.payoutVersion !== 2
        ) {
          continue;
        }

        if (!entry.settled) {
          let allPaid = true;

          for (
            const [uid, amount] of
            Object.entries(
              payouts(entry)
            )
          ) {
            try {
              if (
                !(await payWinner(
                  uid,
                  entry.roundNumber,
                  amount
                ))
              ) {
                allPaid = false;
              }
            } catch (e) {
              allPaid = false;

              console.error(
                "PAYOUT ERROR:",
                uid,
                e.message
              );
            }
          }

          if (!allPaid) {
            continue;
          }

          await gameRef()
            .child(
              "pendingSettlement"
            )
            .child(key)
            .update({
              settled: true,
              settledAt: Date.now()
            });
        }

        const history =
          historyArray(
            (
              await gameRef()
                .child("history")
                .once("value")
            ).val()
          );

        const published =
          history.some(
            x =>
              Number(
                x.roundNumber
              ) === Number(
                entry.roundNumber
              ) &&
              x.hasSettledBets ===
                true
          );

        if (!published) {
          await publishWinners(
            entry
          );
        }
      }
    } catch (e) {
      console.error(
        "SETTLEMENT ERROR:",
        e.message
      );
    } finally {
      settling = false;
    }
  }

  // =====================================
  // GAME SOCKET ONLY
  // =====================================

  io.on("connection", socket => {
    console.log(
      "LION PLAYER:",
      socket.id
    );

    socket.on(
      "greedy_lion_join",
      async () => {
        socket.join(ROOM);

        if (!game) {
          await startGame();
        }

        if (
          !socket.connected ||
          !game
        ) {
          return;
        }

        const s = state();

        socket.emit(
          "greedy_lion_state",
          s
        );

        socket.emit(
          "greedy_lion_clock",
          {
            roundNumber:
              s.roundNumber,
            roundStartsAt:
              s.roundStartsAt,
            roundEndsAt:
              s.roundEndsAt,
            cycleEndsAt:
              s.cycleEndsAt,
            serverTime:
              s.serverTime,
            secondsRemaining:
              s.secondsRemaining,
            bettingEnabled:
              s.bettingEnabled,
            phase: s.phase
          }
        );
      }
    );

    socket.on(
      "greedy_lion_place_bet",
      async (data, ack) => {
        const reply =
          typeof ack === "function"
            ? ack
            : () => {};

        try {
          reply(
            await placeBet(data)
          );
        } catch (e) {
          console.error(
            "SOCKET BET ERROR:",
            e.message
          );

          reply(
            fail("SERVER_ERROR")
          );
        }
      }
    );

    socket.on(
      "greedy_lion_leave",
      () => {
        socket.leave(ROOM);
      }
    );

    socket.on(
      "disconnect",
      () => {
        console.log(
          "LION DISCONNECTED:",
          socket.id
        );
      }
    );
  });

  // =====================================
  // GAME TIMERS
  // =====================================

  setInterval(() => {
    if (!ready) {
      return;
    }

    if (!game) {
      startGame().catch(
        console.error
      );
      return;
    }

    sendClock();

    if (advancing) {
      return;
    }

    const recorded =
      historyArray(
        game.history
      ).some(
        x =>
          Number(
            x.roundNumber
          ) === Number(
            game.roundNumber
          )
      );

    if (
      Date.now() >=
        Number(
          game.roundEndsAt
        ) &&
      !recorded
    ) {
      announceResult().catch(
        console.error
      );
    } else if (
      Date.now() >=
        cycleEnd(game) &&
      recorded
    ) {
      nextRound().catch(
        console.error
      );
    }
  }, 1000);

  setInterval(() => {
    settlePending().catch(
      console.error
    );
  }, 1000);

  // =====================================
  // GAME HEALTH
  // =====================================

  app.get(
    "/api/greedy-lion/status",
    (req, res) => {
      const history =
        historyArray(
          game?.history
        );

      res.json({
        app: "Max Chat",
        service:
          "greedy_lion_only",
        status: "online",
        firebaseReady: ready,
        roundNumber:
          game?.roundNumber ||
          null,
        phase: phase(game),
        secondsRemaining:
          remaining(),
        bettingEnabled:
          ready &&
          BETS_ENABLED &&
          phase(game) ===
            "betting",
        bettingDurationMs:
          BET_MS,
        resultDurationMs:
          RESULT_MS,
        lastResult:
          history.length
            ? history[
                history.length - 1
              ]
            : null
      });
    }
  );

  // =====================================
  // START GAME MODULE
  // =====================================

  console.log(
    "GREEDY LION MODULE ATTACHED"
  );

  startGame()
    .then(() => {
      settlePending().catch(
        console.error
      );
    })
    .catch(
      console.error
    );
}

module.exports = {
  attachGreedyLion
};
