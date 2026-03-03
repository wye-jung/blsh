import asyncio
import blsh.krx.store as krx


if __name__ == "__main__":
    asyncio.run(krx.store_krx_today())
