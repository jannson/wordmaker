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
  keyset.push_back("app");
  keyset.push_back("apple");

  marisa::Trie trie;
  trie.build(keyset);

  marisa::Agent agent;
  agent.set_query("apple");
  while (trie.common_prefix_search(agent)) {
    std::cout.write(agent.key().ptr(), agent.key().length());
    std::cout << ": " << agent.key().id() << std::endl;
  }
  return 0;
}
