// sample.cc
#include <stdint.h>
#include <cstdio>
#include <cstdarg>
#include <cstddef>
#include <cmath>
#include <iostream>

#include <marisa.h>

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
