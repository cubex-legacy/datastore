<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\BeginTransactionRequest;
use api\services\datastore\BlindWriteRequest;
use api\services\datastore\CommitRequest;
use api\services\datastore\CompositeFilter;
use api\services\datastore\Entity;
use api\services\datastore\Filter;
use api\services\datastore\Key;
use api\services\datastore\KindExpression;
use api\services\datastore\LookupRequest;
use api\services\datastore\Mutation;
use api\services\datastore\PartitionId;
use api\services\datastore\Property;
use api\services\datastore\PropertyExpression;
use api\services\datastore\PropertyFilter;
use api\services\datastore\PropertyOrder;
use api\services\datastore\PropertyReference;
use api\services\datastore\Query;
use api\services\datastore\QueryResultBatch;
use api\services\datastore\ReadOptions;
use api\services\datastore\RollbackRequest;
use api\services\datastore\RunQueryRequest;
use api\services\datastore\Value;
use CloudDatastore\GoogleAPI\GoogleServiceOptions;
use Cubex\ServiceManager\IService;
use Cubex\ServiceManager\ServiceConfigTrait;

class DatastoreService implements IService
{
  const DEFAULT_HOST        = 'https://www.googleapis.com';
  const DEFAULT_PK_PASSWORD = 'notasecret';

  use ServiceConfigTrait;

  /**
   * @var Connection
   */
  protected $_connection;
  /**
   * @var string
   */
  protected $_namespace;
  /**
   * @var string
   */
  protected $_transactionId = "";
  /**
   * @var Mutation
   */
  protected $_currentMutation = null;


  public function connect()
  {
    $dataSet = $this->config()->getStr("dataset");
    if(!$dataSet)
    {
      throw new \Exception('CloudDatastore: dataset not configured');
    }
    $options = new GoogleServiceOptions();
    $options->setHost($this->config()->getStr("host", self::DEFAULT_HOST));
    $options->setPrivateKeyFile($this->config()->getStr("privateKeyFile", ""));
    $options->setPrivateKeyPassword(
      $this->config()->getStr("privateKeyPassword", self::DEFAULT_PK_PASSWORD)
    );
    $options->setServiceAccountName(
      $this->config()->getStr("serviceAccount", "")
    );
    $options->setApplicationName(
      $this->config()->getStr("applicationName", "")
    );
    $options->setClientId($this->config()->getStr("clientId", ""));

    $this->_connection = new Connection($options);
    $this->_connection->setDataset($dataSet);
    $this->_namespace = $this->config()->getStr("namespace", "");
  }

  public function disconnect()
  {
    $this->_connection = null;
  }

  /**
   * @return Connection
   */
  public function conn()
  {
    if($this->_connection === null)
    {
      $this->connect();
    }
    return $this->_connection;
  }

  /**
   * @param Key $key
   *
   * @return string
   */
  public function encodeKey(Key $key)
  {
    return base64_encode($key->serialize());
  }

  /**
   * @param string $keyStr
   *
   * @return Key
   */
  public function decodeKey($keyStr)
  {
    $key = new Key();
    $key->parse(base64_decode($keyStr));
    return $key;
  }

  public function keysMatch(Key $keyA, Key $keyB)
  {
    return $this->encodeKey($keyA) == $this->encodeKey($keyB);
  }

  /**
   * Begin a transaction
   *
   * @return string The transaction ID
   * @throws \Exception
   */
  public function beginTransaction()
  {
    if($this->inTransaction())
    {
      throw new \Exception(
        'Already in a transaction. Please commit or rollback before starting a new transaction.'
      );
    }

    $req = new BeginTransactionRequest();
    $resp = $this->conn()->beginTransaction($req);
    $this->_transactionId = $resp->getTransaction();

    $this->_currentMutation = new Mutation();

    return $this->_transactionId;
  }

  public function inTransaction()
  {
    return $this->_transactionId ? true : false;
  }

  /**
   * @return null|Key[] AutoInsertId keys if any were created
   * @throws \Exception
   */
  public function commit()
  {
    if(! $this->inTransaction())
    {
      throw new \Exception('Not in a transaction');
    }

    // TODO: retries on timeouts?
    $req = new CommitRequest();
    $req->setTransaction($this->_transactionId);
    $req->setMutation($this->_currentMutation);
    $resp = $this->conn()->commit($req);

    $this->_transactionId = "";
    $this->_currentMutation = null;

    $mutationResult = $resp->getMutationResult();
    if($mutationResult->hasInsertAutoIdKey())
    {
      $autoIdKeys = $mutationResult->getInsertAutoIdKeyList();
    }
    else
    {
      $autoIdKeys = null;
    }
    return $autoIdKeys;
  }

  public function rollback()
  {
    if($this->inTransaction())
    {
      $req = new RollbackRequest();
      $req->setTransaction($this->_transactionId);
      $this->_transactionId = "";
      $this->_currentMutation = null;
      $this->conn()->rollback($req);
      // TODO: Do something with the response??
    }
  }

  /**
   * @param string  $kind
   * @param string  $name
   * @param array[] $ancestorPath An array where each element is an array
   *                              containing 'kind' and either 'id' or 'name'
   *
   * @return Entity
   */
  public function getEntityByName($kind, $name, $ancestorPath = null)
  {
    $path = $ancestorPath ? $ancestorPath : [];
    $path[] = ['kind' => $kind, 'name' => $name];
    return $this->getEntityByPath($path);
  }

  /**
   * @param string $kind
   * @param int    $id
   * @param null   $ancestorPath
   *
   * @return Entity
   */
  public function getEntityById($kind, $id, $ancestorPath = null)
  {
    $path = $ancestorPath ? $ancestorPath : [];
    $path[] = ['kind' => $kind, 'id' => $id];
    return $this->getEntityByPath($path);
  }

  /**
   * @param array $path
   *
   * @return Entity
   */
  public function getEntityByPath($path)
  {
    return $this->getEntity($this->makeKeyFromPath($path));
  }


  public function getEntitiesByName($kind, $names, $ancestorPath = null)
  {
    $paths = [];
    foreach($names as $name)
    {
      $paths[] = ['kind' => $kind, 'name' => $name];
    }
    return $this->getEntitiesByPath($paths, $ancestorPath);
  }

  public function getEntitiesById($kind, $ids, $ancestorPath = null)
  {
    $paths = [];
    foreach($ids as $id)
    {
      $paths[] = ['kind' => $kind, 'id' => $id];
    }
    return $this->getEntitiesByPath($paths, $ancestorPath);
  }

  /**
   * @param Key $key
   *
   * @return Entity
   */
  public function getEntity(Key $key)
  {
    $res = $this->getEntities([$key]);
    reset($res);
    return current($res);
  }

  /**
   * Get entities by their kind and id/name
   *
   * @param array[]      $childPaths The final path element for each element.
   *                                 This should be an array of arrays
   *                                 where each contains 'kind' and
   *                                 either 'id' or 'name'
   * @param null|array[] $ancestorPath
   *
   * @return Entity[]
   */
  public function getEntitiesByPath($childPaths, $ancestorPath = null)
  {
    $keys = [];
    foreach($childPaths as $childPath)
    {
      $fullPath = $ancestorPath ? $ancestorPath : [];
      $fullPath[] = $childPath;
      $keys[] = $this->makeKeyFromPath($fullPath);
    }

    return $this->getEntities($keys);
  }

  /**
   * Get entities by their ancestor path
   *
   * @param array $ancestorPath An array of path elements in
   *                            ['kind', 'id', 'name'] format
   *
   * @return Entity[]
   */
  public function getEntitiesByAncestorPath($ancestorPath)
  {
    return $this->getEntitiesByAncestor($this->makeKeyFromPath($ancestorPath));
  }

  /**
   * Get entities by their ancestor path
   *
   * @param Key    $ancestorKey
   * @param int    $limit
   * @param string $orderBy
   * @param bool   $orderAscending
   *
   * @return Entity[]
   */
  public function getEntitiesByAncestor(
    Key $ancestorKey, $limit = 0, $orderBy = "", $orderAscending = true
  )
  {
    $propertyFilter = new PropertyFilter();
    $propertyFilter->setProperty(
      (new PropertyReference())->setName('__key__')
    );
    $propertyFilter->setOperator(PropertyFilter\Operator::HAS_ANCESTOR);
    $propertyFilter->setValue(
      (new Value())->setKeyValue($ancestorKey)
    );

    $query = new Query();
    $query->addKind(
      (new KindExpression())->setName('TxTestChild')
    );
    $query->setFilter(
      (new Filter())->setPropertyFilter($propertyFilter)
    );

    if($limit > 0)
    {
      $query->setLimit($limit);
    }

    if($orderBy != "")
    {
      $query->setOrder(
        (new PropertyOrder())
          ->setProperty(
            (new PropertyReference())->setName($orderBy)
          )
          ->setDirection(
            $orderAscending ? PropertyOrder\Direction::ASCENDING :
            PropertyOrder\Direction::DESCENDING
          )
      );
    }

    return $this->runQuery($query);
  }


  /**
   * @param string|string[] $kinds        List of kinds to search for
   * @param array           $properties   List of property => value to search for.
   *                                      Only supports string properties.
   * @param null|array      $ancestorPath The ancestor path to restrict the search to
   * @param array           $orders       List of properties to order by as
   *                                      property name => PropertyOrder\Direction
   * @param int             $limit
   * @param string|string[] $groupBy      Properties to group by
   * @param string|string[] $requiredProperties The list of properties to return
   *                                            in the query. If null then all
   *                                            properties will be returned.
   *                                            If an empty array then only keys.
   *
   * @return Query
   */
  public function buildQuery(
    $kinds, array $properties, $ancestorPath = null, array $orders = [],
    $limit = 0, $groupBy = "", $requiredProperties = ""
  )
  {
    $query = new Query();

    if($kinds)
    {
      if(! is_array($kinds))
      {
        $kinds = [$kinds];
      }
      foreach($kinds as $kind)
      {
        $query->addKind(
          (new KindExpression())->setName($kind)
        );
      }
    }

    $propertyFilters = [];
    if(!empty($properties))
    {
      foreach($properties as $property => $value)
      {
        $propertyFilters[] =
          (new PropertyFilter())
            ->setProperty((new PropertyReference())->setName($property))
            ->setOperator(PropertyFilter\Operator::EQUAL)
            ->setValue((new Value())->setStringValue($value));
      }
    }

    if($ancestorPath)
    {
      $key = $this->makeKeyFromPath($ancestorPath);
      $propertyFilters[] = (new PropertyFilter())
        ->setProperty(
          (new PropertyReference())->setName('__key__')
        )
        ->setOperator(PropertyFilter\Operator::HAS_ANCESTOR)
        ->setValue(
          (new Value())->setKeyValue($key)
        );
    }

    if(count($propertyFilters) > 0)
    {
      $filter = new Filter();
      if(count($propertyFilters) == 1)
      {
        $filter->setPropertyFilter($propertyFilters[0]);
      }
      else
      {
        $compFilter = new CompositeFilter();
        foreach($propertyFilters as $propFilter)
        {
          $compFilter->addFilter(
            (new Filter())->setPropertyFilter($propFilter)
          );
        }
        $filter->setCompositeFilter($compFilter);
      }
      $query->setFilter($filter);
    }

    if(count($orders) > 0)
    {
      foreach($orders as $propName => $direction)
      {
        $query->addOrder(
          (new PropertyOrder())
            ->setProperty($propName)
            ->setDirection($direction)
        );
      }
    }

    if($limit > 0)
    {
      $query->setLimit($limit);
    }

    if(!empty($groupBy))
    {
      if(! is_array($groupBy))
      {
        $groupBy = [$groupBy];
      }

      foreach($groupBy as $propName)
      {
        $query->addGroupBy(
          (new PropertyReference())->setName($propName)
        );
      }
    }

    if(! empty($requiredProperties))
    {
      if(! is_array($requiredProperties))
      {
        $requiredProperties = [$requiredProperties];
      }

      foreach($requiredProperties as $propName)
      {
        $query->addProjection(
          (new PropertyExpression())
            ->setProperty((new PropertyReference())->setName($propName))
        );
      }
    }
    return $query;
  }

  /**
   * Build a query to get one or more entities by key
   *
   * @param string   $kind The kind of entities
   * @param Key[]    $keys The keys of the entities to get
   * @param string[] $requiredProperties A list of required properties. If null
   *                                     then load all properties.
   *
   * @return Query
   * @throws \Exception
   */
  public function buildKeyQuery($kind, array $keys, $requiredProperties = null)
  {
    if(count($keys) == 0)
    {
      throw new \Exception('No keys specified');
    }

    $query = new Query();
    $query->addKind((new KindExpression())->setName($kind));

    $propertyFilters = [];
    foreach($keys as $key)
    {
      $propertyFilters[] = (new PropertyFilter())
        ->setProperty((new PropertyReference)->setName('__key__'))
        ->setOperator(PropertyFilter\Operator::EQUAL)
        ->setValue((new Value())->setKeyValue($key));
    }

    $filter = new Filter();
    if(count($propertyFilters) == 1)
    {
      $filter->setPropertyFilter($propertyFilters[0]);
    }
    else
    {
      $compFilter = new CompositeFilter();
      foreach($propertyFilters as $propFilter)
      {
        $compFilter->addFilter(
          (new Filter())->setPropertyFilter($propFilter)
        );
      }
      $filter->setCompositeFilter($compFilter);
    }
    $query->setFilter($filter);

    if(! empty($requiredProperties))
    {
      foreach($requiredProperties as $propName)
      {
        $query->addProjection(
          (new PropertyExpression())
            ->setProperty((new PropertyReference())->setName($propName))
        );
      }
    }
    return $query;
  }

  /**
   * Run a query and return the results
   *
   * @param Query  $query
   *
   * @return Entity[]
   */
  public function runQuery(Query $query)
  {
    $entities = [];

    $req = new RunQueryRequest();
    $req->setQuery($query);
    if($this->_namespace != "")
    {
      $req->setPartitionId($this->_getPartitionId());
    }
    if($this->inTransaction())
    {
      $req->setReadOptions($this->_getTransactionReadOptions());
    }

    $response = $this->conn()->runQuery($req);
    $entityResults = $response->getBatch()->getEntityResultList();
    foreach($entityResults as $result)
    {
      $entities[] = $result->getEntity();
    }
    return $entities;
  }

  /**
   * Perform a lookup with retries
   *
   * @param Key[]  $keys
   * @param int    $retryLimit Maximum number of retries
   * @param int    $retryNum Internal use: The current retry number
   *
   * @return Entity[] The returned objects, the array keys
   *                  are the encoded Key objects
   * @throws \Exception
   */
  public function getEntities($keys, $retryLimit = 5, $retryNum = 0)
  {
    $result = [];
    $req = new LookupRequest();
    foreach($keys as $key)
    {
      $req->addKey($key);
    }

    if($this->inTransaction())
    {
      $req->setReadOptions($this->_getTransactionReadOptions());
    }

    $response = $this->conn()->lookup($req);
    if($response->hasFound())
    {
      foreach($response->getFoundList() as $entityResult)
      {
        $entity = $entityResult->getEntity();
        $keyStr = $this->encodeKey($entity->getKey());
        $result[$keyStr] = $entity;
      }
    }

    if($response->hasMissing())
    {
      foreach($response->getMissingList() as $entityResult)
      {
        $key = $entityResult->getEntity()->getKey();
        $keyStr = $this->encodeKey($key);
        $result[$keyStr] = null;
      }
    }

    if($response->hasDeferred())
    {
      if($retryNum < $retryLimit)
      {
        $retryNum++;
        sleep($retryNum * 2);
        $retryRes = $this->getEntities(
          $response->getDeferredList(),
          $retryLimit,
          $retryNum
        );
        $result = $result + $retryRes;
      }
      else
      {
        throw new \Exception(
          'Retry limit hit when performing Datastore lookup'
        );
      }
    }

    return $result;
  }

  /**
   * Write a single entity. If a transaction is open the write
   * will be added to the transaction, otherwise it will be performed
   * using a BlindWrite operation
   *
   * @param Entity $entity
   */
  public function writeEntity(Entity $entity)
  {
    $this->writeEntities([$entity], false);
  }

  /**
   * Write multiple entities. If a transaction is open the
   * write will be added to the transaction, otherwise it will
   * be performed using a BlindWrite operation
   *
   * @param Entity[] $entities
   * @param bool     $useTransaction
   *
   * @throws \Exception
   */
  public function writeEntities($entities, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($entities as $entity)
      {
        $this->_currentMutation->addUpsert($entity);
      }
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($entities as $entity)
        {
          $this->_currentMutation->addUpsert($entity);
        }
        $committing = true;
        $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($entities as $entity)
      {
        $mutation->addUpsert($entity);
      }

      $this->_performBlindWrite($mutation);
    }
  }

  public function insertAutoId($entity)
  {
    $keys = $this->insertAutoIdMulti([$entity], false);
    return $keys ? $keys[0] : null;
  }

  /**
   * Insert multiple entities and auto-generate the ID
   *
   * @param Entity[] $entities
   * @param bool     $useTransaction If true then perform the operation in a
   *                                 transaction. Ignored if a transaction
   *                                 is already in progress.
   *
   * @return null|Key[] null if in a transaction, or the new keys if not
   * @throws \Exception
   */
  public function insertAutoIdMulti($entities, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($entities as $entity)
      {
        $this->_currentMutation->addInsertAutoId($entity);
      }
      return null;
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($entities as $entity)
        {
          $this->_currentMutation->addInsertAutoId($entity);
        }
        $committing = true;
        return $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($entities as $entity)
      {
        $mutation->addInsertAutoId($entity);
      }
      $resp = $this->_performBlindWrite($mutation);
      return $resp->getMutationResult()->getInsertAutoIdKeyList();
    }
  }

  /**
   * Delete an entity by its path
   *
   * @param array $path
   */
  public function deleteByPath($path)
  {
    $this->delete($this->makeKeyFromPath($path));
  }

  /**
   * Delete an entity
   *
   * @param Key $key
   */
  public function delete(Key $key)
  {
    $this->deleteMulti([$key], false);
  }

  /**
   * Delete multiple entities
   *
   * @param Key[] $keys
   * @param bool  $useTransaction
   *
   * @throws \Exception
   */
  public function deleteMulti($keys, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($keys as $key)
      {
        $this->_currentMutation->addDelete($key);
      }
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($keys as $key)
        {
          $this->_currentMutation->addDelete($key);
        }
        $committing = true;
        $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($keys as $key)
      {
        $mutation->addDelete($key);
      }
      $this->_performBlindWrite($mutation);
    }
  }

  protected function _performBlindWrite(Mutation $mutation)
  {
    // TODO: retries on timeouts?
    $req = new BlindWriteRequest();
    $req->setMutation($mutation);
    return $this->conn()->blindWrite($req);
  }

  /**
   * @param array $path
   *
   * @return Key
   */
  public function makeKeyFromPath($path)
  {
    $key = new Key();
    foreach($path as $pathElement)
    {
      $pe = new Key\PathElement();
      $pe->setKind($pathElement['kind']);
      if(isset($pathElement['name']))
      {
        $pe->setName($pathElement['name']);
      }
      else if(isset($pathElement['id']))
      {
        $pe->setId($pathElement['id']);
      }
      $key->addPathElement($pe);
    }
    if($this->_namespace != "")
    {
      $key->setPartitionId($this->_getPartitionId());
    }
    return $key;
  }

  /**
   * @param Key $key
   *
   * @return array
   */
  public function makePathFromKey(Key $key)
  {
    $path = [];
    foreach($key->getPathElementList() as $pathElement)
    {
      $pathPart = ['kind' => $pathElement->getKind()];
      if($pathElement->hasId())
      {
        $pathPart['id'] = $pathElement->getId();
      }
      else if($pathElement->hasName())
      {
        $pathPart['name'] = $pathElement->getName();
      }
      $path[] = $pathPart;
    }
    return $path;
  }

  /**
   * @param array $properties An array of property name => value. Everything is
   *                          created as a string property.
   * @param string[] $indexProperties A list of properties to index
   *
   * @return Entity
   */
  public function buildEntity($properties, $indexProperties = [])
  {
    $entity = new Entity();
    foreach($properties as $propName => $value)
    {
      $doIndex = in_array($propName, $indexProperties);
      $property = new Property();
      $property->setName($propName);
      if(is_array($value))
      {
        $property->setMulti(true);
        foreach($value as $thisValue)
        {
          $property->addValue(
            (new Value())->setStringValue($thisValue)->setIndexed($doIndex)
          );
        }
      }
      else
      {
        $property->setMulti(false);
        $property->setValue(
          (new Value())->setStringValue($value)->setIndexed($doIndex)
        );
      }
      $entity->addProperty($property);
    }
    return $entity;
  }

  /**
   * Get all of the properties in an entity
   *
   * @param Entity $entity
   *
   * @return array
   */
  public function entityToArray(Entity $entity)
  {
    $entityData = [];
    foreach($entity->getPropertyList() as $property)
    {
      $propName = $property->getName();
      $propData = [];
      $values = $property->getValueList();
      foreach($values as $value)
      {
        $data = null;
        $set = true;
        if($value->hasStringValue())
        {
          $data = $value->getStringValue();
        }
        else if($value->hasIntegerValue())
        {
          $data = $value->getIntegerValue();
        }
        else if($value->hasBooleanValue())
        {
          $data = $value->getBooleanValue();
        }
        else if($value->hasBlobKeyValue())
        {
          $data = $value->getBlobKeyValue();
        }
        else if($value->hasBlobValue())
        {
          $data = $value->getBlobValue();
        }
        else if($value->hasDoubleValue())
        {
          $data = $value->getDoubleValue();
        }
        else if($value->hasEntityValue())
        {
          $data = $value->getEntityValue();
        }
        else if($value->hasKeyValue())
        {
          $data = $value->getKeyValue();
        }
        else if($value->hasTimestampMicrosecondsValue())
        {
          $data = $value->getTimestampMicrosecondsValue();
        }
        else
        {
          $set = false;
        }
        if($set)
        {
          $propData[] = $data;
        }
      }

      if(count($propData) > 0)
      {
        if(count($propData) == 1)
        {
          $entityData[$propName] = $propData[0];
        }
        else
        {
          $entityData[$propName] = $propData;
        }
      }
    }
    return $entityData;
  }


  protected function _getPartitionId()
  {
    return (new PartitionId())->setNamespace($this->_namespace);
  }

  protected function _getTransactionReadOptions()
  {
    return (new ReadOptions())->setTransaction($this->_transactionId);
  }
}
