from pydantic import BaseModel


class AuthViewerResponse(BaseModel):
    email: str
    displayName: str | None = None
    isAuthenticated: bool
    isDemo: bool
    redditConnected: bool
    redditConnectionMode: str = "none"
    spotifyConnected: bool = False


class RedditConnectStartResponse(BaseModel):
    authorizeUrl: str


class SpotifyConnectStartResponse(BaseModel):
    authorizeUrl: str
