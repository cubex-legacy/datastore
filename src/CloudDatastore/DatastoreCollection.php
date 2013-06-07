<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\Entity;
use api\services\datastore\PropertyOrder;
use Cubex\Mapper\Collection;

class DatastoreCollection extends Collection
{
  protected $_kind;
  protected $_properties = [];
  protected $_ancestorPath = [];
  protected $_limit = 0;
  protected $_offset = 0;
  protected $_orderBy = [];
  protected $_groupBy = "";
  protected $_requiredProperties = "";

  /**
   * @var DatastoreService
   */
  protected $_service = null;

  /**
   * @var DatastoreMapper
   */
  protected $_mapperType;
  /**
   * @var string
   */
  protected $_mapperClass;

  public function __construct(DatastoreMapper $map, array $mappers = null)
  {
    parent::__construct($map, $mappers);
    $this->_kind = $map->kind();
    $this->_service = $map->connection();
    $this->_mapperClass = get_class($map);
  }

  public function loadWhere($properties /* , $arg, $arg, $arg ... */)
  {
    $this->_properties = $properties;
    return $this;
  }

  public function setAncestorPath(array $ancestorPath)
  {
    $this->_ancestorPath = $ancestorPath;
    return $this;
  }

  public function addAncestorPath($kind, $id = null, $name = null)
  {
    $pathComponent = ['kind' => $kind];
    if($id !== null)
    {
      $pathComponent['id'] = $id;
    }
    else if($name !== null)
    {
      $pathComponent['name'] = $name;
    }
    $this->_ancestorPath[] = $pathComponent;
    return $this;
  }

  public function setLimit($offset = 0, $limit = 100)
  {
    $this->_offset = (int)$offset;
    $this->_limit = (int)$limit;
    return $this;
  }

  public function setOrderBy($field, $order = 'ASC')
  {
    $this->_orderBy = [];
    return $this->addOrderBy($field, $order);
  }

  public function addOrderBy($field, $order = 'ASC')
  {
    $direction = strtolower(trim($order)) == 'desc' ?
      PropertyOrder\Direction::DESCENDING : PropertyOrder\Direction::ASCENDING;
    $this->_orderBy[$field] = $direction;
    return $this;
  }

  public function addOrderByKey($order = 'ASC')
  {
    return $this->addOrderBy('__key__', $order);
  }

  public function setOrderByKey($order = 'ASC')
  {
    return $this->setOrderBy('__key__', $order);
  }

  public function setGroupBy($groupBy)
  {
    $this->_groupBy = $groupBy;
    return $this;
  }

  /**
   * @param string[] $requiredProperties
   */
  public function setRequiredProperties(array $requiredProperties)
  {
    $this->_requiredProperties = $requiredProperties;
  }

  public function clearRequiredProperties()
  {
    $this->_requiredProperties = "";
  }

  protected function _buildQuery()
  {
    return $this->_service->buildQuery(
      $this->_kind,
      $this->_properties,
      $this->_ancestorPath,
      $this->_orderBy,
      $this->_limit,
      $this->_offset,
      $this->_groupBy,
      $this->_requiredProperties
    );
  }

  protected function _preCheckMappers()
  {
    if(!$this->isLoaded())
    {
      $this->get();
    }
  }

  public function get()
  {
    $query = $this->_buildQuery();
    $entities = $this->_service->runQuery($query);
    $this->addEntities($entities);
    $this->setLoaded(true);
    return $this;
  }

  public function addEntity(Entity $entity)
  {
    $this->addMapper((new $this->_mapperClass())->setEntity($entity));
  }

  /**
   * @param Entity[] $entities
   */
  public function addEntities($entities)
  {
    foreach($entities as $entity)
    {
      $this->addEntity($entity);
    }
  }

  /**
   * Load entities by their key paths. Note this will clear the collection
   * and replace its contents with these entities.
   *
   * @param array[] $paths A list of paths where each path element contains
   *                       'kind' and either 'id' or 'name'
   *
   * @return $this
   */
  public function loadByPaths($paths)
  {
    $this->clear();
    $keys = [];
    foreach($paths as $path)
    {
      $keys[] = $this->_service->makeKeyFromPath($path);
    }
    $this->addEntities($this->_service->getEntities($keys));
    $this->setLoaded(true);
    return $this;
  }
}
