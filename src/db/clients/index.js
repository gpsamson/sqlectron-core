import kissmetrics from './kissmetrics';


/**
 * List of supported database clients
 */
export const CLIENTS = [
  {
    key: 'kissmetrics',
    name: 'Kissmetrics',
    disabledFeatures: [
      'server:ssl',
      'server:socketPath',
      'server:schema',
      'server:host',
      'server:port',
      'server:domain',
      'scriptCreateTable',
      'cancelQuery',
      'server:ssh',
    ],
  },
];


export default {
  kissmetrics,
};
