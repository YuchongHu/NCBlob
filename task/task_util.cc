#include "task_util.hh"
#include "meta.hpp"

#include <algorithm>
#include <ctime>
#include <random>

std::vector<meta::chunk_index_t> genRandomList(int n, int k, int id) {
  std::vector<meta::chunk_index_t> res{};
  res.reserve(n);
  for (int i = 0; i < n; i++) {
    if (i != id) {
      res.push_back(i);
    }
  }

  std::srand(unsigned(std::time(0)));

  std::shuffle(
      res.begin(), res.end(), std::default_random_engine(std::time(0)));
  res.resize(k);
  return res;
}