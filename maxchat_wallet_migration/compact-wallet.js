'use strict';
// Exact arithmetic for staging. Does not activate Wallet V2 or migrate balances.
const AMOUNT_RE = /^(0|[1-9][0-9]{0,60})$/;
const MAX = 10n ** 60n - 1n;
const OP_RE = /^[a-zA-Z0-9_-]{16,100}$/;
function amount(x) {
  if (typeof x !== 'string' && typeof x !== 'bigint' && !(typeof x === 'number' && Number.isSafeInteger(x))) throw new Error('UNSAFE_AMOUNT');
  const s = String(x);
  if (!AMOUNT_RE.test(s)) throw new Error('INVALID_AMOUNT');
  const n = BigInt(s);
  if (n > MAX) throw new Error('AMOUNT_TOO_LARGE');
  return n;
}
function createWallet(initial) {
  return {balance:String(amount(initial)),version:1,debits:{},credits:{}};
}
function operate(wallet,kind,opId,rawAmount) {
  if (!OP_RE.test(opId)) throw new Error('INVALID_OPERATION_ID');
  const a = amount(rawAmount);
  if (a === 0n) throw new Error(kind === 'debit' ? 'ZERO_BET' : 'ZERO_PAYOUT');
  if (!wallet || typeof wallet.balance !== 'string') throw new Error('INVALID_WALLET');
  const own = kind === 'debit' ? 'debits' : 'credits';
  const opposite = kind === 'debit' ? 'credits' : 'debits';
  if (wallet[opposite]?.[opId] !== undefined) throw new Error('OPERATION_ID_REUSED');
  if (wallet[own]?.[opId] !== undefined) {
    if (amount(wallet[own][opId]) !== a) throw new Error('OPERATION_ID_AMOUNT_CONFLICT');
    return {wallet,repeated:true};
  }
  const b = amount(wallet.balance);
  if (kind === 'debit' && b < a) throw new Error('INSUFFICIENT_COINS');
  const result = kind === 'debit' ? b-a : b+a;
  if (result > MAX) throw new Error('BALANCE_TOO_LARGE');
  return { wallet:{...wallet,balance:String(result),[own]:{...(wallet[own] || {}),[opId]:String(a)},version:(wallet.version||0)+1},repeated:false};
}
function debit(w,id,a){return operate(w,'debit',id,a);}
function credit(w,id,a){return operate(w,'credit',id,a);}
function payoutForBets(bets,winner,multiplier){
  if(!Number.isInteger(winner)||winner<0||winner>7) throw Error('BAD_WINNER');
  const x=amount(multiplier);let total=0n;
  for(const b of bets){if(Number(b.foodIndex)===winner)total+=amount(b.amount)*x;if(total>MAX)throw Error('PAYOUT_TOO_LARGE');}
  return String(total);
}
module.exports={amount,createWallet,debit,credit,payoutForBets};
