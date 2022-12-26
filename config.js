module.exports = {
  clientId: process.env.AIRTHINGS_CLIENT_ID,
  clientSecret: process.env.AIRTHINGS_CLIENT_SECRET,
  persistAccesstokenPath: process.env.ACCESS_TOKEN_PATH,
  scope: process.env.AIRTHINGS_CLIENT_SCOPE,
  persistLatestSamples: process.env.LATEST_SAMPLES_PATH,
  cacheLatestSamplesFor: process.env.CACHE_LATEST_SAMPLES_FOR,
  listenPort: process.env.LISTEN_PORT,
};
