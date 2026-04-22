from pydantic import BaseModel


class AuthViewerResponse(BaseModel):
    email: str
    displayName: str | None = None
    isAuthenticated: bool
    isDemo: bool
    redditConnected: bool
    redditConnectionMode: str = "none"


class RedditConnectStartResponse(BaseModel):
    authorizeUrl: str
