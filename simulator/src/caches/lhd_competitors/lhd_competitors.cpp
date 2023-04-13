#include <unordered_map>
#include <random>
#include <cmath>
#include <cassert>
#include "lhd_competitors.h"
#include "cache.hpp"
#include "repl.hpp"
#include "parser.hpp"
#include "hyperbolic.hpp"
#include "gds.hpp"
#include "ranked_lru.hpp"
#include "gdwheel.hpp"
#include "gdwheelcost.hpp"
#include "constants.hpp"

/*
  LHD variants impl
*/

LHDBase::LHDBase()
{
    lhdcache = new cache_competitors::Cache();
    lhdcache->availableCapacity = (uint64_t)1 * 1024 * 1024;
    lhdcache->repl = new repl_competitors::LRU();
}

void LHDBase::setSize(const uint64_t &cs) {
    _cacheSize = cs;
    lhdcache->availableCapacity = cs;
}

bool LHDBase::lookup(const SimpleRequest &req)
{
    const parser_competitors::Request preq {1, (int64_t)req.size, (int64_t)req.id, (int64_t)req.cost};
    return(lhdcache->access(preq));
}

void LHDBase::admit(const SimpleRequest &req)
{
    // nop
}

void LHDBase::evict()
{
    // nop
}

LHDHyperbolic::LHDHyperbolic()
{
    lhdcache = new cache_competitors::Cache();
    lhdcache->availableCapacity = (uint64_t)1 * 1024 * 1024;
    int assoc = 64;
    int admissionSamples = 8;
    lhdcache->repl = new repl_competitors::RankedHyperbolicSizeAware(assoc, admissionSamples, lhdcache);
}

LHDSAMPLEDGDSF::LHDSAMPLEDGDSF()
{
    lhdcache = new cache_competitors::Cache();
    lhdcache->availableCapacity = (uint64_t)1 * 1024 * 1024;
    int assoc = 64;
    int admissionSamples = 8;
    lhdcache->repl = new repl_competitors::RankedGreedyDualSizeFreq(assoc, admissionSamples);
}

LHDGDWheel::LHDGDWheel()
{
    lhdcache = new cache_competitors::Cache();
    lhdcache->availableCapacity = (uint64_t)1 * 1024 * 1024;
    lhdcache->repl = new repl_competitors::GDWheel();
}

LHDGDWheelCost::LHDGDWheelCost()
{
    lhdcache = new cache_competitors::Cache();
    lhdcache->availableCapacity = (uint64_t)1 * 1024 * 1024;
    lhdcache->repl = new repl_competitors::GDWheelCost();
}
