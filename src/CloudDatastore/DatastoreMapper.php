<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\Entity;
use api\services\datastore\Filter;
use api\services\datastore\Key;
use api\services\datastore\KindExpression;
use api\services\datastore\Property;
use api\services\datastore\PropertyFilter;
use api\services\datastore\Query;
use api\services\datastore\Value;
use CloudDatastore\Facade\CloudDatastore;
use Cubex\Mapper\DataMapper;

class DatastoreMapper extends DataMapper
{
  protected $_autoTimestamp = false;
  protected $_attributeType = '\CloudDatastore\DatastoreAttribute';

  /**
   * The name of the datastore service to use
   * @var string
   */
  protected $_datastoreConnection = 'datastore';
  /**
   * The default ancestor path to use for entities of this type.
   * This should be an array of path elements where each element is an array
   * with fields 'kind' and either 'id' or 'name', e.g.
   * [['kind' => 'TestElement', 'name' => 'ParentElement'] ... ]
   * @var null|array
   */
  protected $_defaultAncestorPath = null;
  /**
   * If this is true then the ID is a string used as the Name property.
   * If it is false then ID is an integer used as the Datastore ID.
   * @var bool
   */
  protected $_idIsName = true;
  /**
   * The name of the Kind represented by this mapper
   * @var string
   */
  protected $_kind;
  /**
   * The actual ancestor path of the object. This may or may not be the
   * same as the defaultAncestorPath above.
   * @var null|array
   */
  protected $_ancestorPath = null;
  /**
   * The actual Entity
   * @var Entity
   */
  protected $_entity = null;
  /**
   * The entity's key
   * @var Key
   */
  protected $_key = null;
  /**
   * True if the data has been loaded
   * @var bool
   */
  protected $_loaded;
  /**
   * A list of attributes that have been set
   * @var string[]
   */
  protected $_setAttributes = [];

  public function connection()
  {
    return CloudDatastore::getAccessor($this->_datastoreConnection);
  }

  public function __construct($id = null, $ancestorPath = null)
  {
    if(empty($this->_kind))
    {
      throw new \Exception('ERROR: Mapper does not specify a Kind');
    }

    parent::__construct($id);

    if($ancestorPath === null)
    {
      $ancestorPath = $this->_defaultAncestorPath;
    }
    $this->setAncestorPath($ancestorPath);
    $this->_loaded = false;
  }

  public function kind()
  {
    return $this->_kind;
  }

  public function setAncestorPath($ancestorPath)
  {
    $this->_pathChanged();
    $this->_ancestorPath = $ancestorPath;
    return $this;
  }

  public function ancestorPath()
  {
    return $this->_ancestorPath;
  }

  public function setId($id)
  {
    $this->_pathChanged();
    return parent::setId($id);
  }

  protected function _pathChanged()
  {
    $this->_key = null;
    $this->_loaded = false;
  }

  public function key()
  {
    if($this->_key === null)
    {
      $this->_key = $this->connection()->makeKeyFromPath(
        $this->_getEntityPath()
      );
    }
    return $this->_key;
  }

  public function entity()
  {
    if($this->_entity === null)
    {
      $this->load($this->id(), $this->ancestorPath());
    }
    return $this->_entity;
  }

  public function load($id = null, $ancestorPath = null)
  {
    if(! $this->_loaded)
    {
      $this->_load($id, $ancestorPath);
    }
  }

  protected function _load(
    $id = null, $ancestorPath = null, $requiredAttributes = null
  )
  {
    if($id !== null)
    {
      $this->setId($id);
    }
    if($ancestorPath !== null)
    {
      $this->setAncestorPath($ancestorPath);
    }

    $entity = $this->connection()->getEntity($this->key());
    if($entity)
    {
      $this->_entity = $entity;
      $this->hydrateFromEntity($this->_entity, $requiredAttributes);
      $this->setExists(true);
    }
    else
    {
      $this->setExists(false);
      $this->_entity = null;
    }
    $this->_loaded = true;
  }

  public function delete()
  {
    $this->connection()->delete($this->key());
    $this->_loaded = true;
    $this->setExists(false);
    $this->_entity = null;
  }

  /**
   * Load only the specified attributes/properties
   * @param string[] $attributes
   */
  public function loadSpecificAttributes(array $attributes)
  {
    // are all of the required attributes indexed?
    $allIndexed = true;
    foreach($attributes as $attrName)
    {
      $attr = $this->_attribute($attrName);
      if((!($attr instanceof DatastoreAttribute)) || (!$attr->index()))
      {
        $allIndexed = false;
        break;
      }
    }

    if($allIndexed)
    {
      // All required properties are indexed so use a projection query
      $query = $this->connection()->buildKeyQuery(
        $this->kind(), [$this->key()], $attributes
      );
      $entities = $this->connection()->runQuery($query);
      if($entities && (count($entities) > 0))
      {
        $this->hydrateFromEntity($entities[0]);
      }
    }
    else
    {
      // Not all properties are indexed so we have to load the whole entity
      $this->_load(null, null, $attributes);
    }
  }

  /**
   * Load any required attributes which have not been set manually
   */
  protected function _loadUnsetAttributes()
  {
    $missingAttributes = [];
    foreach($this->getRawAttributes() as $attribute)
    {
      if($attribute instanceof DatastoreAttribute)
      {
        $attrName = $attribute->name();
        if((!$attribute->optional()) &&
          (!in_array($attrName, $this->_setAttributes))
        )
        {
          $missingAttributes[] = $attrName;
        }
      }
    }

    if(count($missingAttributes) > 0)
    {
      $this->loadSpecificAttributes($missingAttributes);
    }
  }

  public function setData(
    $attribute, $value, $serialized = false, $bypassValidation = false
  )
  {
    parent::setData($attribute, $value, $serialized, $bypassValidation);
    $this->_setAttributes[] = $attribute;
  }

  public function getData($attribute)
  {
    if(! $this->_loaded)
    {
      $this->load();
    }
    return parent::getData($attribute);
  }

  public function exists()
  {
    if(! $this->_loaded)
    {
      $this->load();
    }
    return parent::exists();
  }

  public function saveChanges(
    $validate = false, $processAll = false, $failFirst = false
  )
  {
    if(! $this->_loaded)
    {
      $this->_loadUnsetAttributes();
    }

    $this->_saveValidation(
      $validate,
      $processAll,
      $failFirst
    );

    $entity = new Entity();
    $entity->setKey($this->key());
    foreach($this->getRawAttributes() as $attribute)
    {
      if($attribute instanceof DatastoreAttribute)
      {
        $value = new Value();
        $value->setIndexed($attribute->index());
        $data = $attribute->rawData();

        if(empty($data) && $attribute->optional())
        {
          continue;
        }

        switch($attribute->type())
        {
          case DatastoreAttribute::TYPE_STRING:
            $value->setStringValue($data);
            break;
          case DatastoreAttribute::TYPE_INT:
            $value->setIntegerValue($data);
            break;
          case DatastoreAttribute::TYPE_BOOL:
            $value->setBooleanValue($data);
            break;
          case DatastoreAttribute::TYPE_DOUBLE:
            $value->setDoubleValue($data);
            break;
          case DatastoreAttribute::TYPE_TIMESTAMP:
            $value->setTimestampMicrosecondsValue($data);
            break;
          case DatastoreAttribute::TYPE_BLOB_KEY:
            $value->setBlobKeyValue($data);
            break;
          case DatastoreAttribute::TYPE_BLOB:
            $value->setBlobValue($data);
            break;
          case DatastoreAttribute::TYPE_ENTITY:
            $value->setEntityValue($data);
            break;
          case DatastoreAttribute::TYPE_KEY:
            $value->setKeyValue($data);
            break;
        }

        $entity->addProperty(
          (new Property())
            ->setName($attribute->name())
            ->setMulti($attribute->multi())
            ->setValue($value)
        );
      }
    }

    $this->connection()->writeEntity($entity);
    $this->_entity = $entity;
  }

  public function setEntity(Entity $entity)
  {
    $this->_entity = $entity;
    $this->hydrateFromEntity($entity);
  }

  public function hydrateFromEntity(Entity $entity, $requiredAttributes = null)
  {
    $data = $this->connection()->entityToArray($entity);
    $filteredData = [];
    if($requiredAttributes !== null)
    {
      foreach($requiredAttributes as $attrName)
      {
        if(isset($data[$attrName]))
        {
          $filteredData[$attrName] = $data[$attrName];
        }
      }
    }
    else
    {
      $filteredData = $data;
    }
    $this->hydrate($filteredData);
  }

  protected function _getEntityPath()
  {
    $path = $this->_ancestorPath ? $this->_ancestorPath : [];

    $finalPath = ['kind' => $this->kind()];
    if($this->_idIsName)
    {
      $finalPath['name'] = $this->id();
    }
    else
    {
      $finalPath['id'] = $this->id();
    }
    $path[] = $finalPath;
    return $path;
  }
}