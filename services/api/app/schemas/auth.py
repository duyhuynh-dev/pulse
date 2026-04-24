from pydantic import BaseModel


class AuthViewerResponse(BaseModel):
    userId: str
    email: str
    displayName: str | None = None
    isAuthenticated: bool
    isDemo: bool
    authMethod: str = "demo"
    redditConnected: bool
    redditConnectionMode: str = "none"
    spotifyConnected: bool = False


class RedditConnectStartResponse(BaseModel):
    authorizeUrl: str


class SpotifyConnectStartResponse(BaseModel):
    authorizeUrl: str
