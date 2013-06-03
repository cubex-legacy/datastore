<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use Cubex\Data\Attribute\Attribute;

class DatastoreAttribute extends Attribute
{
  const TYPE_STRING    = 1;
  const TYPE_INT       = 2;
  const TYPE_BOOL      = 3;
  const TYPE_DOUBLE    = 4;
  const TYPE_TIMESTAMP = 5;
  const TYPE_BLOB_KEY  = 6;
  const TYPE_BLOB      = 7;
  const TYPE_ENTITY    = 8;
  const TYPE_KEY       = 9;

  protected $_type = self::TYPE_STRING;
  protected $_index = false;
  protected $_multi = false;

  /**
   * @param int $type One of the DatastoreAttribute::TYPE_* constants
   */
  public function setType($type)
  {
    $this->_type = $type;
  }

  /**
   * @return int
   */
  public function type()
  {
    return $this->_type;
  }

  /**
   * @param bool $index True if this attribute should be indexed
   */
  public function setIndex($index)
  {
    $this->_index = $index;
  }

  /**
   * @return bool
   */
  public function index()
  {
    return $this->_index;
  }

  /**
   * @param bool $multi True if this attribute can take multiple values
   *
   * @throws \Exception
   */
  public function setMulti($multi)
  {
    if($multi)
    {
      throw new \Exception('Multiple values are not yet supported');
    }
    $this->_multi = $multi;
  }

  /**
   * @return bool
   */
  public function multi()
  {
    return $this->_multi;
  }
}
