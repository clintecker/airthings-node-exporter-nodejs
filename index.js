/**
 * TODO: Could query slightly more often if we use the detector ids from the
 *       cached samples.
 */
const net = require('net');
const { cloneDeep } = require('lodash');
const fs = require('fs');
const { ClientCredentials } = require('simple-oauth2');
const Wreck = require('@hapi/wreck');
const config = require('./config');

const notMetrics = ['time', 'relayDeviceType'];

if (!config.clientId) {
  throw new Error('Airthings client ID is missing');
}
if (!config.clientSecret) {
  throw new Error('Airthings client secret is missing');
}
if (!config.persistAccesstokenPath) {
  throw new Error('Airthings access token path is missing');
}
if (!config.scope) {
  throw new Error('Airthings scope is missing');
}
if (!config.persistLatestSamples) {
  throw new Error('Airthings latest samples path is missing');
}
if (!config.cacheLatestSamplesFor) {
  throw new Error('Airthings latest samples cache time is missing');
}

const logLevels = {
  trace: 0,
  debug: 10,
  info: 20,
  log: 20,
  warn: 30,
  error: 40,
};

const cacheLatestSamplesForMs = parseInt(config.cacheLatestSamplesFor, 10) * 1000;
const clientConfiguration = {
  client: {
    id: config.clientId,
    secret: config.clientSecret,
  },
  auth: {
    tokenHost: 'https://accounts.airthings.com',
    tokenPath: 'https://accounts-api.airthings.com/v1/token',
  },
};

const log = (level, message, ...args) => {
  const logLevelLimitName = config.logLevel;
  if (logLevels[level] >= logLevels[logLevelLimitName.toLowerCase()]) {
    console[level](`[${(Date.now() / 1000.0).toFixed(3)}] [${level.toUpperCase()}] ${message}`, ...args);
  }
};

const persistAccessToken = async (accessToken) => {
  const accessTokenJson = JSON.stringify(accessToken, null, 2);
  log('debug', 'Caching access token');
  try {
    fs.writeFileSync(config.persistAccesstokenPath, accessTokenJson, {
      encoding: 'utf8',
      mode: 0o600,
      flag: 'w',
    });
  } catch (error) {
    log('log', 'Error persisting access token', error.message);
  }
};

const getPersistedAccesstoken = async () => {
  let accessToken;
  try {
    const accessTokenJson = fs.readFileSync(config.persistAccesstokenPath, {
      encoding: 'utf8',
      flag: 'r',
    });
    accessToken = JSON.parse(accessTokenJson);
  } catch (error) {
    log('error', 'Error reading access token', error.message);
    return null;
  }
  return accessToken;
};

const persistLatestSamples = async (latestSamples) => {
  const latestSamplesJson = JSON.stringify({
    persistedAt: new Date(),
    samples: latestSamples,
  }, null, 2);
  log('debug', 'Caching latest samples');
  try {
    fs.writeFileSync(config.persistLatestSamples, latestSamplesJson, {
      encoding: 'utf8',
      mode: 0o600,
      flag: 'w',
    });
  } catch (error) {
    log('error', 'Error persisting latest samples', error.message);
  }
};

const getPersistedLatestSamples = async () => {
  let latestSamples;
  log('debug', 'Fetching cached samples');
  try {
    const latestSamplesJson = fs.readFileSync(config.persistLatestSamples, {
      encoding: 'utf8',
      flag: 'r',
    });
    latestSamples = JSON.parse(latestSamplesJson);
  } catch (error) {
    log('error', 'Error reading latest samples', error.message);
    return null;
  }
  return latestSamples;
};

const fetchNewToken = async (client) => {
  const tokenParams = {
    scope: config.scope,
  };
  let accessToken;
  try {
    accessToken = await client.getToken(tokenParams);
  } catch (error) {
    log('error', 'Access Token error', error.message);
    throw error;
  }
  return accessToken;
};

const login = async (client) => {
  const accessTokenJSONString = await getPersistedAccesstoken();
  let accessToken;
  if (accessTokenJSONString) {
    log('debug', 'Found cached token.');
    accessToken = client.createToken(accessTokenJSONString);
  } else {
    log('debug', 'No cached token, fetching fresh token');
    accessToken = await fetchNewToken(client);
    await persistAccessToken(accessToken);
  }
  return accessToken;
};

const ensureFreshToken = async (accessToken, client) => {
  let newAccessToken = accessToken;
  if (accessToken.expired()) {
    try {
      newAccessToken = await fetchNewToken(client);
      await persistAccessToken(newAccessToken);
    } catch (error) {
      log('error', 'Error refreshing token: ', error.message);
      throw new Error(`Error refreshing token: ${error.message}`);
    }
  }
  return newAccessToken;
};

const getData = async (resourcePath, accessToken, client) => {
  const freshAccessToken = await ensureFreshToken(accessToken, client);
  let payload;
  try {
    const options = {
      headers: { Authorization: freshAccessToken.token.access_token },
    };
    log('debug',
      `Making request to https://ext-api.airthings.com/v1/${resourcePath}`,
    );
    const response = await Wreck.get(
      `https://ext-api.airthings.com/v1/${resourcePath}`,
      options,
    );
    payload = response.payload;
  } catch (error) {
    log('error', 'Error fetching data', error.message);
    return null;
  }
  return JSON.parse(payload.toString());
};

const deviceIsNotAHub = (device) => device.deviceType !== 'HUB';

const decorateDevice = (device) => ({
  ...device,
  segment: {
    ...device.segment,
    started: new Date(Date.parse(device.segment.started)),
  },
});

const metricToString = (detector) =>
  ([metricName, metricValue]) =>
    `airthings_${metricName}{device_id="${detector.id}",device_location="${detector.location.name}",device_segment="${detector.segment.name}"} ${metricValue}`;

const refreshMetrics = async (accessToken, cachedLatestSamples, client) => {
  const latestSamples = cloneDeep(cachedLatestSamples);
  // Enumerate Devices
  const devicesPayload = await getData('devices', accessToken, client);
  if (!devicesPayload) {
    log('error', 'Error fetching devices');
    return latestSamples;
  }
  const { devices } = devicesPayload;
  // Filter out Hubs and Decorate Data
  const detectors = devices.filter(deviceIsNotAHub).reduce(
    (accumulator, device) => ({
      ...accumulator,
      [device.id]: decorateDevice(device),
    }),
    {},
  );

  // Debugging Data
  log('debug', `Found ${devices.length} devices`);
  log('debug', ` - ${Object.keys(detectors).length} detectors`);
  log('debug', ` - ${devices.length - Object.keys(detectors).length} hubs`);

  // Fetch the latest samples for each detector and store on the detector.
  try {
    const detectorSamples = await Promise.all(Object.values(detectors).map(
      async (detector) => {
        const devicePayload = await getData(
          `devices/${detector.id}/latest-samples`,
          accessToken,
        );
        if (devicePayload) {
          return [detector.id, { ...devicePayload.data, time: new Date(parseInt(devicePayload.data.time, 10) * 1000.0) }];
        }
        return [null, null];
      },
    ));
    // log('debug', `Detector samples: ${JSON.stringify(detectorSamples, null, 2)}`)
    // Overwrite cached values with fresh values if we got them.
    detectorSamples
      .filter(([id, samples]) => id !== null && samples !== null)
      .forEach(([id, samples]) => {
        latestSamples[id] = { ...detectors[id], latestSamples: samples };
      });
  } catch (error) {
    log('error', 'Error fetching latest samples', error.message);
  }
  // Cache latest samples.
  await persistLatestSamples(latestSamples);
  return latestSamples;
};

const getMetrics = async (client) => {
  let useCache = false;
  let cachedLatestSamples = {};

  // Obtain Access Token
  const accessToken = await login(client);
  const persistedSamplesResponse = await getPersistedLatestSamples();
  if (persistedSamplesResponse) {
    log('debug', 'Found cached latest samples');
    const { persistedAt } = persistedSamplesResponse;
    if (persistedAt) {
      const latestSamplesCacheAge = new Date() - Date.parse(persistedAt);
      cachedLatestSamples = persistedSamplesResponse.samples;
      if (latestSamplesCacheAge && latestSamplesCacheAge <= cacheLatestSamplesForMs) {
        log('debug', `Cached samples are ${latestSamplesCacheAge}ms old, within cache window of ${cacheLatestSamplesForMs}ms`);
        useCache = true;
      } else {
        log('debug', `Cached samples are ${latestSamplesCacheAge}ms old, outside cache window of ${cacheLatestSamplesForMs}ms`);
      }
    } else {
      log('warn', 'Persisted samples don\'t have a timestamp.');
    }
  } else {
    log('debug', 'No cached latest samples');
  }
  if (!useCache) {
    // Trigger Refresh of Metrics
    cachedLatestSamples = await refreshMetrics(accessToken, cachedLatestSamples, client);
  } else {
    log('debug', 'Using cached latest samples');
  }
  // log('debug', `cachedLatestSamples ${JSON.stringify(cachedLatestSamples, null, 2)}`)
  // Produce node metrics.
  const metrics = Object.entries(cachedLatestSamples).reduce((acc, [, detector]) => {
    const metricLines = Object.entries(detector.latestSamples)
      .filter(([metricName]) => !notMetrics.includes(metricName))
      .map(metricToString(detector));
    return [...acc, ...metricLines];
  }, []);
  return metrics;
};

const onConnectionClose = ({ remoteAddress, connectionTimeout }) => {
  log('info', `Connection from ${remoteAddress} closed`);
  clearTimeout(connectionTimeout);
};

const onConnectionError = ({ err, remoteAddress }) => {
  log('debug', `Connection ${remoteAddress} error: ${err.message} `);
};

const onConnectionData = ({ remoteAddress, connection, client, connectionTimeout }) => {
  log('debug', `Connection ${remoteAddress} asking for data`);
  clearTimeout(connectionTimeout)
  try {
    getMetrics(client).then((metrics) => {
      connection.write('HTTP/1.1 200 OK\n');
      connection.write('Content-Type: text/plain\n');
      connection.write('Connection: close\n\n');
      connection.write(metrics.join('\n'));
      connection.end();
    });
  } catch (error) {
    log('error', 'Error fetching metrics', error.message);
    connection.end();
  }
};

const handleConnection = (connection, client) => {
  const remoteAddress = `${connection.remoteAddress}:${connection.remotePort}`;
  log('info', `New connection from ${remoteAddress}`);

  const connectionTimeout = setTimeout(() => {
    log('info', `Closing idle connection from ${remoteAddress}`);
    connection.end();
  }, 1000);

  connection.setEncoding('utf8');
  connection.on('data', (data) => onConnectionData({
    data, remoteAddress, connection, client, connectionTimeout
  }));
  connection.once('close', () => onConnectionClose({ remoteAddress, connectionTimeout }));
  connection.on('error', (err) => onConnectionError({ err, remoteAddress }));
};

const main = () => {
  log('debug', `Client ID: ${config.clientId}`);
  log('debug', `Client Scope: ${config.scope}`);
  log('debug', `Persisting Token to: ${config.persistAccesstokenPath}`);
  log('debug', `Persisting Latest Samples to: ${config.persistLatestSamples} for ${cacheLatestSamplesForMs} milliseconds.`);
  const client = new ClientCredentials(clientConfiguration);
  const server = net.createServer();
  server.on('connection', (connection) => handleConnection(connection, client));
  server.listen(config.listenPort, '0.0.0.0', () => {
    log('info', `Server listening on ${server.address().address}:${config.listenPort}`);
  });
};

main();
