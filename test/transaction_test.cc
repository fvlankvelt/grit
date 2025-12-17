#include <filesystem>

#include "storage.h"

void ConflictTest() {
    std::filesystem::remove_all("/tmp/graphdb");
    Storage storage("/tmp/graphdb");

    std::shared_ptr txMgr(std::make_shared<TransactionManager>());
    txMgr->Initialize(storage.db, storage.transactions);

    WriteContext tx1Ctx(storage.db, 1);
    auto tx1 = txMgr->OpenForWrite(tx1Ctx);
    txMgr->Touch(*tx1, tx1Ctx, {"c", 1});

    WriteContext tx2Ctx(storage.db, 2);
    auto tx2 = txMgr->OpenForWrite(tx1Ctx);
    txMgr->Touch(*tx2, tx2Ctx, {"c", 1});
    txMgr->Commit(*tx2, tx2Ctx);

    try {
        txMgr->Commit(*tx1, tx1Ctx);
        assert(false);
    } catch (TransactionException e) {
        assert(e == TX_CONFLICT);
    }
}

void ProgressTest() {
    std::filesystem::remove_all("/tmp/graphdb");
    Storage storage("/tmp/graphdb");

    std::shared_ptr txMgr(std::make_shared<TransactionManager>());
    txMgr->Initialize(storage.db, storage.transactions);

    WriteContext tx1Ctx(storage.db, 1);
    auto tx1 = txMgr->OpenForWrite(tx1Ctx);
    txMgr->Touch(*tx1, tx1Ctx, {"c", 1});

    txMgr->Commit(*tx1, tx1Ctx);

    try {
        txMgr->Commit(*tx1, tx1Ctx);
        assert(false);
    } catch (TransactionException e) {
        assert(e == TX_NOT_IN_PROGRESS);
    }
}

void RestartTest() {
    std::filesystem::remove_all("/tmp/graphdb");
    uint64_t tx_id;
    {
        Storage storage("/tmp/graphdb");

        std::shared_ptr txMgr(std::make_shared<TransactionManager>());
        txMgr->Initialize(storage.db, storage.transactions);

        WriteContext tx1Ctx(storage.db, 1);
        auto tx1 = txMgr->OpenForWrite(tx1Ctx);
        txMgr->Touch(*tx1, tx1Ctx, {"c", 1});

        WriteContext tx2Ctx(storage.db, 2);
        auto tx2 = txMgr->OpenForWrite(tx2Ctx);
        txMgr->Touch(*tx2, tx2Ctx, {"c", 1});
        txMgr->Rollback(tx2->GetTxId(), tx2Ctx);

        WriteContext tx3Ctx(storage.db, 3);
        auto tx3 = txMgr->OpenForWrite(tx3Ctx);
        tx_id = tx3->GetTxId();
        txMgr->Touch(*tx3, tx3Ctx, {"c", 1});

        {
            WriteContext ctx(storage.db, 4);
            txMgr->Commit(*tx1, ctx);
        }
    }

    {
        Storage storage("/tmp/graphdb");

        std::shared_ptr txMgr(std::make_shared<TransactionManager>());
        txMgr->Initialize(storage.db, storage.transactions);

        WriteContext ctx(storage.db, 5);
        auto tx = txMgr->GetWriteTxn(tx_id);
        try {
            txMgr->Commit(*tx, ctx);
            assert(false);
        } catch (TransactionException e) {
            assert(e == TX_CONFLICT);
        }
    }
}

int main() {
    ConflictTest();
    ProgressTest();
    RestartTest();
}
