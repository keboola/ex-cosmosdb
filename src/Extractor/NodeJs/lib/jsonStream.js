'use strict';

import fs from 'fs';
// We are using separated file descriptor to output JSON documents
// Default file descriptors are used: STDOUT to log info messages and STDERR to log warnings.
// Number of the custom file descriptor is loaded from ENV (set by PHP), ... fallback is STDOUT.
let jsonStreamFd;
if (process.env.JSON_STREAM_FD !== undefined) {
  jsonStreamFd = parseInt(process.env.JSON_STREAM_FD, 10);
} else {
  console.error('Please, set env variable "JSON_STREAM_FD". Using STDOUT as fallback.');
  jsonStreamFd = process.stdout.fd;
}
export default fs.createWriteStream(null, { fd: jsonStreamFd });
