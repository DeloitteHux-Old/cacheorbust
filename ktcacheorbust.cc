#include "config.h"

#include "ktcacheorbust.h"

using namespace cob;

const char* DEFAULT_HOST = "";
const uint32_t DEFAULT_PORT = 6080;
const uint32_t DEFAULT_SERVER_THREADS = 16;
const uint32_t DEFAULT_FETCHER_THREADS = 16;
const uint32_t DEFAULT_TTL = 3600;

const char FLAG_PENDING = 1 << 0;

void CacheOrBust::configure(kt::TimedDB* dbary, size_t dbnum,
    kt::ThreadedServer::Logger* logger, uint32_t logkinds,
    const char* expr)
{
  _assert_(dbary && logger && expr);

  // use the 0th database
  _db = dbary;

  _logger = logger;
  _logkinds = logkinds;
  _serv.set_logger(logger, logkinds);
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust starting up...");

  _host = DEFAULT_HOST;
  _port = DEFAULT_PORT;
  _server_threads = DEFAULT_SERVER_THREADS;
  _fetcher_threads = DEFAULT_FETCHER_THREADS;
  _ttl = DEFAULT_TTL;
  _use_keepalive = true;

  std::vector<std::string> elems;
  kc::strsplit(expr, '#', &elems);
  std::vector<std::string>::iterator it = elems.begin();
  std::vector<std::string>::iterator itend = elems.end();
  while (it != itend) {
    std::vector<std::string> fields;
    if (kc::strsplit(*it, '=', &fields) > 1) {
      const char* key = fields[0].c_str();

      // Join everything back together with =
      std::string temp_value;
      std::vector<std::string>::iterator fit = fields.begin();
      std::vector<std::string>::iterator fitend = fields.end();
      ++fit; // Ignore the first value
      temp_value += *fit++;
      for (; fit != fitend; ++fit) temp_value.append("=").append(*fit);

      const char* value = temp_value.c_str();
      if (!std::strcmp(key, "host")) {
        _host = value;
      } else if (!std::strcmp(key, "port")) {
        _port = kc::atoi(value);
      } else if (!std::strcmp(key, "url_prefix")) {
        _url_prefix = value;
      } else if (!std::strcmp(key, "server_threads")) {
        _server_threads = kc::atoi(value);
      } else if (!std::strcmp(key, "fetcher_threads")) {
        _fetcher_threads = kc::atoi(value);
      } else if (!std::strcmp(key, "ttl")) {
        _ttl = kc::atoi(value);
      } else if (!std::strcmp(key, "keepalive")) {
        if (!std::strcmp(value, "true")) {
          _use_keepalive = true;
        } else if (!std::strcmp(value, "false")) {
          _use_keepalive = false;
        } else {
          log(kt::ThreadedServer::Logger::ERROR, "keepalive value must be 'true' or 'false' (assuming 'true')");
        }
      } else {
        std::stringstream err;
        err << "CacheOrBust: unknown option '" << key << "'";
        log(kt::ThreadedServer::Logger::ERROR, err.str().c_str());
      }
    }
    ++it;
  }

  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust parameters:");
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: host='%s'", _host.c_str());
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: port='%d'", _port);
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: url_prefix='%s'", _url_prefix.c_str());
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: server_threads='%d'", _server_threads);
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: fetcher_threads='%d'", _fetcher_threads);
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: ttl='%d'", _ttl);
  log(kt::ThreadedServer::Logger::SYSTEM, "CacheOrBust: keepalive='%s'", _use_keepalive ? "true" : "false");

}

bool CacheOrBust::start()
{
  std::string addr;
  if (!_host.empty()) {
    addr = kt::Socket::get_host_address(_host);
    if (addr.empty()) {
      log(kt::ThreadedServer::Logger::ERROR, "unknown host: %s", _host.c_str());
      return false;
    }
  }

  srand((unsigned long)(kc::time() * 1000));
  _queue = new FetchQueue(this, _db, _fetcher_threads, _use_keepalive);
  _queue->start();

  std::string listen;
  kc::strprintf(&listen, "%s:%d", addr.c_str(), _port);
  _serv.set_network(listen, 30);
  _worker = new Worker(this, _server_threads);
  _serv.set_worker(_worker, _server_threads);
  return _serv.start();
}

bool CacheOrBust::stop()
{
  return _serv.stop();
}

bool CacheOrBust::finish()
{
  bool err = false;
  if (!_serv.finish()) err = true;
  delete _worker;
  return !err;
}

bool CacheOrBust::Worker::process(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess)
{
  kt::TimedDB* db = _serv->_db;
  bool success = false;
  char line[16384];

  if (sess->receive_line(line, sizeof(line))) {
    std::vector<std::string> tokens;
    kt::strtokenize(line, &tokens);
    const std::string& cmd = tokens.empty() ? "" : tokens.front();
    if (cmd == "get") {
      success = do_get(serv, sess, tokens, db);
    } else if (cmd == "stats") {
      success = do_stats(serv, sess, tokens, db);
    } else if (cmd == "flush_all") {
      success = do_flush(serv, sess, tokens, db);
    } else if (cmd == "version") {
      sess->printf("VERSION CacheOrBust/%s,KyotoTycoon/%s\r\n", PACKAGE_VERSION, kt::VERSION);
    } else if (cmd == "quit") {
      success = false;
    } else {
      sess->printf("ERROR\r\n");
      serv->log(kt::ThreadedServer::Logger::ERROR, "(%s): invalid command '%s'",
          sess->expression().c_str(), cmd.c_str());
      success = true;
    }
  }

  return success;
}

/* Handle a GET request.
 *
 * GET requests are formed like:
 *
 *    get key url [TTL]
 *
 * If a record for `key` exists, return it immediately. Otherwise
 * enqueue a request for `url`, and store the response body in `key`
 * for `TTL` seconds.
 */
bool CacheOrBust::Worker::do_get(
    kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
    const std::vector<std::string>& tokens, kt::TimedDB* db)
{
  uint32_t tid = sess->thread_id();
  int32_t ttl = _serv->_ttl;

  if (tokens.size() < 2)
    return sess->printf("CLIENT_ERROR missing key\r\n");
  if (tokens.size() > 4)
    return sess->printf("CLIENT_ERROR extra data after TTL\r\n");

  std::string key(tokens[1]);
  if (! key.compare(0, 4, "ias:")) {
    // Strip leading "ias:" from key (for mcrouter prefix-based routing)
    key.erase(0, 4);
  }

  size_t datasize;
  char* data = db->get(key.data(), key.size(), &datasize);
  if (data) {
    const char flags = data[0];
    if (flags & FLAG_PENDING) {
      _opcounts[tid][MISS]++;
      sess->printf("END\r\n");
    } else {
      _opcounts[tid][HIT]++;
      sess->printf("VALUE %s 0 %llu\r\n", key.c_str(), datasize - 1);
      sess->send(data + 1, datasize - 1);
      sess->printf("\r\nEND\r\n");
    }
    delete[] data;
  } else {
    _opcounts[tid][MISS]++;

    // add sentinel record, TTL 30s so that another
    // cache miss in 30s will cause another background
    // fetch to be enqueued
    std::string value(1, FLAG_PENDING);
    if (!db->set(key, value, 30)) {
      sess->printf("SERVER_ERROR could not set sentinel\r\n");
      return true;
    }

    std::string decoded_key(tokens[1]);
    if (! key.compare(0, 4, "http")) {
      // Assume the string is base64 encoded if it doesn't start with "http"
      std::string temp_decoded = b64decode(key);
      // Super basic sanity check of decoded key.  Min length of http://a
      if (temp_decoded.size() >= 8)
        decoded_key = temp_decoded;
    }

    std::string url;
    if (tokens.size() >= 3)
      url = tokens[2];
    else if (!_serv->_url_prefix.empty())
      url = _serv->_url_prefix + decoded_key;
    else
      return sess->printf("CLIENT_ERROR missing URL\r\n");

    sess->printf("END\r\n");

    // NOTE: ttl is not supported for url_prefix mode, only here for backwards compatibility
    if (tokens.size() == 4)
      ttl = kc::atoi(tokens[3].c_str());

    FetchTask* fetch = new FetchTask(key, url, ttl);
    _serv->_queue->add_task(fetch);
    _opcounts[tid][ENQUEUE]++;
  }
  return true;
}

bool CacheOrBust::Worker::do_flush(
    kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
    const std::vector<std::string>& tokens, kt::TimedDB* db)
{
  uint32_t tid = sess->thread_id();
  _opcounts[tid][FLUSH]++;
  db->clear();
  sess->printf("OK\r\n");
  return true;
}

bool CacheOrBust::Worker::do_stats(
    kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
    const std::vector<std::string>& tokens, kt::TimedDB* db)
{
  double now = kc::time();
  std::string result;

  kc::strprintf(&result, "STAT pid %lld\r\n", (long long)kc::getpid());
  kc::strprintf(&result, "STAT uptime %lld\r\n", (long long)(now - _serv->_stime));
  kc::strprintf(&result, "STAT time %lld\r\n", (long long)now);
  kc::strprintf(&result, "STAT version CacheOrBust/%s,KyotoTycoon/%s\r\n", PACKAGE_VERSION, kt::VERSION);
  kc::strprintf(&result, "STAT pointer_size %d\r\n", (int)(sizeof(void*) * 8));
  kc::strprintf(&result, "STAT curr_connections %d\r\n", (int)serv->connection_count());
  kc::strprintf(&result, "STAT threads %d\r\n", (int)_nthreads);
  kc::strprintf(&result, "STAT curr_items %lld\r\n", (long long)db->count());
  kc::strprintf(&result, "STAT bytes %lld\r\n", (long long)db->size());

  OpCounts ops;
  for (int32_t j = 0; j < LAST_OP_; j++) {
    ops[j] = _serv->_opcounts[j];
  }
  for (int32_t i = 0; i < _nthreads; i++) {
    for (int32_t j = 0; j < LAST_OP_; j++) {
      ops[j] += _opcounts[i][j];
    }
  }

  kc::strprintf(&result, "STAT flush %lld\r\n", ops[FLUSH]);
  kc::strprintf(&result, "STAT hit %lld\r\n", ops[HIT]);
  kc::strprintf(&result, "STAT miss %lld\r\n", ops[MISS]);
  if (ops[HIT] || ops[MISS])
    kc::strprintf(&result, "STAT hit_rate %f\r\n", (float)ops[HIT] / (ops[MISS] + ops[HIT]));
  else
    kc::strprintf(&result, "STAT hit_rate 0.0\r\n");
  kc::strprintf(&result, "STAT enqueue %lld\r\n", ops[ENQUEUE]);
  kc::strprintf(&result, "STAT queue_size %lld\r\n", _serv->_queue->count());
  kc::strprintf(&result, "STAT fetch %lld\r\n", ops[FETCH]);
  kc::strprintf(&result, "STAT fetch_failed %lld\r\n", ops[FETCH_FAIL]);

  kc::strprintf(&result, "END\r\n");
  return !!(sess->send(result.data(), result.size()));
}

// From: https://stackoverflow.com/a/37109258
static const int B64index [256] = { 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 62, 63, 62, 62, 63, 52, 53, 54, 55,
 56, 57, 58, 59, 60, 61,  0,  0,  0,  0,  0,  0,  0,  0,  1,  2,  3,  4,  5,  6,
  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,  0,
  0,  0,  0, 63,  0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51 };

std::string CacheOrBust::Worker::b64decode(const void* data, const size_t len)
{
  unsigned char* p = (unsigned char*)data;
  int pad = len > 0 && (len % 4 || p[len - 1] == '=');
  const size_t L = ((len + 3) / 4 - pad) * 4;
  std::string str(L / 4 * 3 + pad, '\0');

  for (size_t i = 0, j = 0; i < L; i += 4)
  {
    int n = B64index[p[i]] << 18 | B64index[p[i + 1]] << 12 | B64index[p[i + 2]] << 6 | B64index[p[i + 3]];
    str[j++] = n >> 16;
    str[j++] = n >> 8 & 0xFF;
    str[j++] = n & 0xFF;
  }
  if (pad)
  {
    int n = B64index[p[L]] << 18 | B64index[p[L + 1]] << 12;
    str[str.size() - 1] = n >> 16;

    if (len > L + 2 && p[L + 2] != '=')
    {
      n |= B64index[p[L + 2]] << 6;
      str.push_back(n >> 8 & 0xFF);
    }
  }
  return str;
}

std::string CacheOrBust::Worker::b64decode(const std::string& str64)
{
  return b64decode(str64.c_str(), str64.size());
}

void CacheOrBust::count_op(Op op)
{
  _opcounts[op]++;
}


// initializer called by the main server
extern "C" void* ktservinit() {
  return new CacheOrBust;
}

// vim:filetype=cpp ts=2 sts=2 sw=2 expandtab
