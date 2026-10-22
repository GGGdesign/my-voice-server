'use strict';
// STAGING ONLY. Never enables itself or changes a live user's balance.
// Call only after an explicitly managed cutover. Firebase Admin SDK bypasses rules.
const {createWallet,debit,credit} = require('./compact-wallet');
function createWalletStore(adminDatabase) {
  const refFor = uid => {if(!/^[a-zA-Z0-9_-]{1,100}$/.test(uid)) throw Error('INVALID_UID'); return adminDatabase.ref('walletV2').child(uid);};
  async function initIfEmpty(uid, verifiedSourceBalance) {
    const ref = refFor(uid); const initial = createWallet(verifiedSourceBalance);
    const tx = await ref.transaction(old => old == null ? initial : undefined,undefined,false);
    return {initialized:tx.committed, wallet:tx.snapshot.val()};
  }
  async function commit(uid,kind,id,value) {
    const ref=refFor(uid); let operation;
    const tx=await ref.transaction(old=>{
      if(!old || typeof old.balance!=='string') return;
      operation=(kind==='debit'?debit:credit)(old,id,value);
      return operation.repeated ? undefined : operation.wallet;
    },undefined,false);
    if(!tx.snapshot.exists()) throw Error('WALLET_NOT_MIGRATED');
    const recorded=kind==='debit'?tx.snapshot.val().debits?.[id]:tx.snapshot.val().credits?.[id];
    if(recorded !== String(value)) throw Error('WALLET_OPERATION_NOT_COMMITTED');
    return {balance:tx.snapshot.val().balance, newlyApplied:tx.committed};
  }
  return {refFor,initIfEmpty,debit:(uid,id,value)=>commit(uid,'debit',id,value),credit:(uid,id,value)=>commit(uid,'credit',id,value)};
}
module.exports={createWalletStore};
