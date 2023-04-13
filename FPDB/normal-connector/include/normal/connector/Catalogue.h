//
//  27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H

#include <string>
#include <unordered_map>
#include <memory>

#include <tl/expected.hpp>

#include <normal/connector/Connector.h>
#include <normal/connector/CatalogueEntry.h>

namespace normal::connector {

class CatalogueEntry;

class Catalogue {

public:
  explicit Catalogue(std::string Name, std::shared_ptr<Connector> Connector);
  virtual ~Catalogue() = default;

  [[nodiscard]] const std::string &getName() const;
  const std::shared_ptr<Connector> &getConnector() const;

  void put(const std::shared_ptr<CatalogueEntry> &entry);
  tl::expected<std::shared_ptr<CatalogueEntry>, std::string> entry(const std::string &name);

  std::string toString();

private:
  std::string name_;
  std::unordered_map<std::string, std::shared_ptr<CatalogueEntry>> entries_;
  std::shared_ptr<Connector> connector_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H
