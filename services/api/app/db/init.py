from sqlalchemy import select

from app.db.base import Base
from app.db.session import AsyncSessionLocal, engine
from app.models.user import EmailPreference, User
from app.services.seed import ensure_demo_catalog, seed_demo_state


async def init_db() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).limit(1))
        if user is None:
            await seed_demo_state(session)
        else:
            preference = await session.scalar(select(EmailPreference).where(EmailPreference.user_id == user.id))
            if preference is None:
                await seed_demo_state(session, only_missing=True)
        await ensure_demo_catalog(session)
        await session.commit()
