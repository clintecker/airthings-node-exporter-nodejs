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

const cacheLatestSamplesForMs = parseInt(config.cacheLatestSamplesFor, 10) * 1000;

console.log(`Client ID: ${config.clientId}`);
console.log(`Client Scope: ${config.scope}`);
console.log(`Persisting Token to: ${config.persistAccesstokenPath}`);
console.log(`Persisting Latest Samples to: ${config.persistLatestSamples} for ${cacheLatestSamplesForMs} milliseconds.`);

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

const client = new ClientCredentials(clientConfiguration);

const persistAccessToken = async (accessToken) => {
  const accessTokenJson = JSON.stringify(accessToken, null, 2);
  console.debug('Caching access token');
  try {
    fs.writeFileSync(config.persistAccesstokenPath, accessTokenJson, {
      encoding: 'utf8',
      mode: 0o600,
      flag: 'w',
    });
  } catch (error) {
    console.log('Error persisting access token', error.message);
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
    console.log('Error reading access token', error.message);
    return null;
  }
  return accessToken;
};

const persistLatestSamples = async (latestSamples) => {
  const latestSamplesJson = JSON.stringify({
    persistedAt: new Date(),
    samples: latestSamples,
  }, null, 2);
  console.debug('Caching latest samples');
  try {
    fs.writeFileSync(config.persistLatestSamples, latestSamplesJson, {
      encoding: 'utf8',
      mode: 0o600,
      flag: 'w',
    });
  } catch (error) {
    console.log('Error persisting latest samples', error.message);
  }
};

const getPersistedLatestSamples = async () => {
  let latestSamples;
  console.debug('Fetching cached samples');
  try {
    const latestSamplesJson = fs.readFileSync(config.persistLatestSamples, {
      encoding: 'utf8',
      flag: 'r',
    });
    latestSamples = JSON.parse(latestSamplesJson);
  } catch (error) {
    console.log('Error reading latest samples', error.message);
    return null;
  }
  return latestSamples;
};

const fetchNewToken = async () => {
  const tokenParams = {
    scope: config.scope,
  };
  let accessToken;
  try {
    accessToken = await client.getToken(tokenParams);
  } catch (error) {
    console.log('Access Token error', error.message);
    console.log(error);
  }
  return accessToken;
};

const login = async () => {
  const accessTokenJSONString = await getPersistedAccesstoken();
  let accessToken;
  if (accessTokenJSONString) {
    console.debug('Found cached token.');
    accessToken = client.createToken(accessTokenJSONString);
  } else {
    console.debug('No cached token, fetching fetch token');
    accessToken = await fetchNewToken();
    await persistAccessToken(accessToken);
  }
  return accessToken;
};

const ensureFreshToken = async (accessToken) => {
  let newAccessToken = accessToken;
  if (accessToken.expired()) {
    try {
      newAccessToken = await fetchNewToken();
      await persistAccessToken(newAccessToken);
    } catch (error) {
      console.log('Error refreshing token: ', error.message);
      throw new Error(`Error refreshing token: ${error.message}`);
    }
  }
  return newAccessToken;
};

const getData = async (resourcePath, accessToken) => {
  const freshAccessToken = await ensureFreshToken(accessToken);
  let payload;
  try {
    const options = {
      headers: { Authorization: freshAccessToken.token.access_token },
    };
    console.debug(
      `Making request to https://ext-api.airthings.com/v1/${resourcePath}`,
    );
    const response = await Wreck.get(
      `https://ext-api.airthings.com/v1/${resourcePath}`,
      options,
    );
    payload = response.payload;
  } catch (error) {
    console.error('Error fetching data', error.message);
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

const refreshMetrics = async (accessToken, cachedLatestSamples) => {
  const latestSamples = cloneDeep(cachedLatestSamples);
  // Enumerate Devices
  const { devices } = await getData('devices', accessToken);
  // Filter out Hubs and Decorate Data
  const detectors = devices.filter(deviceIsNotAHub).reduce(
    (accumulator, device) => ({
      ...accumulator,
      [device.id]: decorateDevice(device),
    }),
    {},
  );

  // Debugging Data
  console.log(`Found ${devices.length} devices`);
  console.log(` - ${Object.keys(detectors).length} detectors`);
  console.log(` - ${devices.length - Object.keys(detectors).length} hubs`);
  // Fetch the latest samples for each detector and store on the detector.
  const detectorSamples = await Promise.all(Object.values(detectors).map(
    async (detector) => {
      const devicePayload = await getData(
        `devices/${detector.id}/latest-samples`,
        accessToken,
      );
      if (devicePayload) {
        return [detector.id, { ...devicePayload.data, time: new Date(parseInt(devicePayload.data.time, 10) * 1000.0) }];
      }
      return null;
    },
  ));
  // Overwrite cached values with fresh values if we got them.
  detectorSamples.forEach(([id, samples]) => {
    latestSamples[id] = { ...detectors[id], latestSamples: samples };
  });
  // Cache latest samples.
  await persistLatestSamples(latestSamples);
  return latestSamples;
};

const getMetrics = async () => {
  let useCache = false;
  let cachedLatestSamples = {};

  // Obtain Access Token
  const accessToken = await login();
  const persistedSamplesResponse = await getPersistedLatestSamples();
  if (persistedSamplesResponse) {
    console.debug('Found cached latest samples');
    const { persistedAt } = persistedSamplesResponse;
    if (persistedAt) {
      const latestSamplesCacheAge = new Date() - Date.parse(persistedAt);
      cachedLatestSamples = persistedSamplesResponse.samples;
      if (latestSamplesCacheAge && latestSamplesCacheAge <= cacheLatestSamplesForMs) {
        console.debug(`Cached samples are ${latestSamplesCacheAge}ms old, within cache window of ${cacheLatestSamplesForMs}ms`);
        useCache = true;
      } else {
        console.debug(`Cached samples are ${latestSamplesCacheAge}ms old, outside cache window of ${cacheLatestSamplesForMs}ms`);
      }
    } else {
      console.debug('Persisted samples don\'t have a timestamp.')
    }
  } else {
    console.debug('No cached latest samples');
  }
  if (!useCache) {
    // Trigger Refresh of Metrics
    cachedLatestSamples = await refreshMetrics(accessToken, cachedLatestSamples);
  } else {
    console.log('Using cached latest samples');
  }
  // Produce node metrics.
  const metrics = Object.entries(cachedLatestSamples).reduce((acc, [, detector]) => {
    const metricLines = Object.entries(detector.latestSamples)
      .filter(([metricName]) => !notMetrics.includes(metricName))
      .map(metricToString(detector));
    return [...acc, ...metricLines];
  }, []);
  return metrics;
};

const onConnectionClose = ({ remoteAddress }) => {
  console.log(`Connection from ${remoteAddress} closed`);
};

const onConnectionError = ({ err, remoteAddress }) => {
  console.error(`Connection ${remoteAddress} error: ${err.message}`);
};

const onConnectionData = ({ remoteAddress, connection }) => {
  console.log(`Connection ${remoteAddress} asking for data`); try {
    getMetrics().then((metrics) => {
      connection.write('HTTP/1.1 200 OK\n');
      connection.write('Content-Type: text/plain\n');
      connection.write('Connection: close\n\n');
      connection.write(metrics.join('\n'));
      connection.end();
    });
  } catch (error) {
    console.error('Error fetching metrics', error.message);
    connection.end();
  }
};

const handleConnection = (connection) => {
  const remoteAddress = `${connection.remoteAddress}:${connection.remotePort}`;
  console.log(`New client connection from ${remoteAddress}`);
  connection.setEncoding('utf8');
  connection.on('data', (data) => onConnectionData({ data, remoteAddress, connection }));
  connection.once('close', () => onConnectionClose({ remoteAddress }));
  connection.on('error', (err) => onConnectionError({ err, remoteAddress }));

};
const server = net.createServer();
server.on('connection', handleConnection);
server.listen(config.listenPort, '0.0.0.0', () => {
  console.debug(`Server listening on ${server.address().address}:${config.listenPort}`);
});
