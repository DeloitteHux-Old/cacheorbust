#ifndef KTCOBTASK_H
#define KTCOBTASK_H

#include <kcthread.h>
#include <kthttp.h>
#include <kttimeddb.h>

namespace kc = kyotocabinet;
namespace kt = kyototycoon;

namespace cob {
  class CacheOrBust;
  class FetchTask;

  class FetchQueue : public kc::TaskQueue {
    typedef std::pair<kt::HTTPClient*, bool> entry;
    typedef std::multimap<std::string, entry>::iterator clientiter;

    private:
      // borrowed
      CacheOrBust* _serv;
      kt::TimedDB* _db;

      // owned
      bool _use_keepalive;
      std::multimap<std::string, entry> _clients;
      kc::Mutex _lock;

      size_t _nthreads;

    public:
      FetchQueue(CacheOrBust* serv, kt::TimedDB* db, size_t nthreads, bool use_keepalive) :
        _serv(serv),
        _db(db),
        _use_keepalive(use_keepalive),
        _clients(),
        _lock(),
        _nthreads(nthreads)
      { }

      void start() { kc::TaskQueue::start(_nthreads); };
      void do_task(kc::TaskQueue::Task* task);

      kt::HTTPClient* get_client(kt::URL& url);
      void return_client(kt::URL& url, kt::HTTPClient* client, bool keep=true);
  };

  class FetchTask : public kc::TaskQueue::Task {
    friend class FetchQueue;

    private:
      std::string _key;
      std::string _url;
      int32_t _ttl;

    public:
      FetchTask(std::string& key, std::string& url, int32_t ttl) :
        _key(key),
        _url(url),
        _ttl(ttl)
      { };
  };
};

#endif
// vim:filetype=cpp ts=2 sts=2 sw=2 expandtab
