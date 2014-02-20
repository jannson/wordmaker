// sample.cc
#include <stdint.h>
#include <cstdio>
#include <cstdarg>
#include <cstddef>
#include <cmath>
#include <iostream>

#include <marisa.h>

#if 0
int dump(const marisa::Trie &trie) {
  std::size_t num_keys = 0;
  marisa::Agent agent;
  agent.set_query("");
  try {
    while (trie.predictive_search(agent)) {
      std::cout.write(agent.key().ptr(), agent.key().length()) << delimiter;
      if (!std::cout) {
        std::cerr << "error: failed to write results to standard output"
            << std::endl;
        return 20;
      }
      ++num_keys;
    }
  } catch (const marisa::Exception &ex) {
    std::cerr << ex.what() << ": predictive_search() failed" << std::endl;
    return 21;
  }
  std::cerr << "#keys: " << num_keys << std::endl;
  return 0;
}
#endif

int main() {
  marisa::Keyset keyset;
  keyset.push_back("a");
  keyset.push_back("apple");
  keyset.push_back("bpple");
  keyset.push_back("apple");
  keyset.push_back("appled");
  keyset.push_back("app");

  marisa::Trie trie;
  trie.build(keyset);
  
  std::cout<<"common preffix search"<<std::endl;
  marisa::Agent agent;
  agent.set_query("apple");
  while (trie.common_prefix_search(agent)) {
    std::cout.write(agent.key().ptr(), agent.key().length());
    std::cout << ": " << agent.key().id() << std::endl;
  }

  std::cout<<"predictive search"<<std::endl;
  while (trie.predictive_search(agent)) {
    std::cout.write(agent.key().ptr(), agent.key().length());
    std::cout << ": " << agent.key().id() << std::endl;
  }

  return 0;
}
