import asyncio

async def say_hello():
    print("Hello World")


async def delayed_hello():
    print("Hello")
    await asyncio.sleep(1)
    print("world")
    
loop = asyncio.get_event_loop()
#loop.run_until_complete(say_hello())
loop.run_until_complete(delayed_hello())
loop.close()
