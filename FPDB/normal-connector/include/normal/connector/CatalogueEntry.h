//
//  27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H

#include <memory>
#include <string>

#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/connector/Catalogue.h>

namespace normal::connector {

class Catalogue;

class CatalogueEntry {

private:
  std::string alias_;
  std::shared_ptr<Catalogue> catalogue_;

public:
  explicit CatalogueEntry(std::string Alias,
                          std::shared_ptr<Catalogue> Catalogue);
  virtual ~CatalogueEntry() = default;

  [[nodiscard]] const std::string &getAlias() const;
  [[nodiscard]] const std::shared_ptr<Catalogue> &getCatalogue() const;

  virtual std::shared_ptr<normal::plan::operator_::ScanLogicalOperator> toLogicalOperator() = 0;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H
