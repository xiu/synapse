from typing import Optional

from synapse.api.errors import SynapseError
from synapse.storage._base import SQLBaseStore
from synapse.types import get_localpart_from_id


class OpenIdStore(SQLBaseStore):
    async def insert_open_id_token(
        self, token: str, ts_valid_until_ms: int, user_id: str
    ) -> None:
        await self.db_pool.simple_insert(
            table="open_id_tokens",
            values={
                "token": token,
                "ts_valid_until_ms": ts_valid_until_ms,
                "user_id": user_id,
            },
            desc="insert_open_id_token",
        )

    async def get_user_email(self, user_id: str) -> Optional[str]:
        return await self.db_pool.simple_select_one_onecol(
            table="user_threepids",
            keyvalues={"user_id": user_id, "medium": "email"},
            retcol="address",
            allow_none=True,
            desc="simple_select_one_onecol",
        )

    async def get_user_id_for_open_id_token(
        self, token: str, ts_now_ms: int
    ) -> Optional[str]:
        def get_user_id_for_token_txn(txn):
            sql = (
                "SELECT user_id FROM open_id_tokens"
                " WHERE token = ? AND ? <= ts_valid_until_ms"
            )

            txn.execute(sql, (token, ts_now_ms))

            rows = txn.fetchall()
            if not rows:
                return None
            else:
                return rows[0][0]

        return await self.db_pool.runInteraction(
            "get_user_id_for_token", get_user_id_for_token_txn
        )

    async def get_user_name(self, user_id: str) -> Optional[str]:
        try:
            localpart = get_localpart_from_id(user_id)
        except SynapseError:
            return None
        return await self.db_pool.simple_select_one_onecol(
            table="profiles",
            keyvalues={"user_id": localpart},
            retcol="displayname",
            allow_none=True,
            desc="simple_select_one_onecol",
        )
