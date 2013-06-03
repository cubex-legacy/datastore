<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\AllocateIdsRequest;
use api\services\datastore\AllocateIdsResponse;
use api\services\datastore\BeginTransactionRequest;
use api\services\datastore\BeginTransactionResponse;
use api\services\datastore\BlindWriteRequest;
use api\services\datastore\BlindWriteResponse;
use api\services\datastore\CommitRequest;
use api\services\datastore\CommitResponse;
use api\services\datastore\LookupRequest;
use api\services\datastore\LookupResponse;
use api\services\datastore\RollbackRequest;
use api\services\datastore\RollbackResponse;
use api\services\datastore\RunQueryRequest;
use api\services\datastore\RunQueryResponse;
use CloudDatastore\GoogleAPI\GoogleService;

class Connection extends GoogleService
{
  protected $_dataset;

  public function setDataset($dataset)
  {
    $this->_dataset = $dataset;
  }

  public function getDataset()
  {
    return $this->_dataset;
  }

  protected function _getBaseUrl()
  {
    return '/datastore/v1beta1/datasets';
  }

  protected function _getUrlForMethod($methodName)
  {
    return $this->_getFullBaseUrl() . '/' . $this->_dataset . '/' . $methodName;
  }

  protected function _getOAuthScopes()
  {
    return array(
      'https://www.googleapis.com/auth/datastore',
      'https://www.googleapis.com/auth/userinfo.email'
    );
  }

  /**
   * @param BlindWriteRequest $request
   *
   * @return BlindWriteResponse
   */
  public function blindWrite(BlindWriteRequest $request)
  {
    $response = new BlindWriteResponse();
    $this->_callMethod('blindWrite', $request, $response);
    return $response;
  }

  /**
   * @param LookupRequest $request
   *
   * @return LookupResponse
   */
  public function lookup(LookupRequest $request)
  {
    $response = new LookupResponse();
    $this->_callMethod('lookup', $request, $response);
    return $response;
  }

  /**
   * @param AllocateIdsRequest $request
   *
   * @return AllocateIdsResponse
   */
  public function allocateIds(AllocateIdsRequest $request)
  {
    $response = new AllocateIdsResponse();
    $this->_callMethod('allocateIds', $request, $response);
    return $response;
  }

  /**
   * @param BeginTransactionRequest $request
   *
   * @return BeginTransactionResponse
   */
  public function beginTransaction(BeginTransactionRequest $request)
  {
    $response = new BeginTransactionResponse();
    $this->_callMethod('beginTransaction', $request, $response);
    return $response;
  }

  /**
   * @param CommitRequest $request
   *
   * @return CommitResponse
   */
  public function commit(CommitRequest $request)
  {
    $response = new CommitResponse();
    $this->_callMethod('commit', $request, $response);
    return $response;
  }

  /**
   * @param RollbackRequest $request
   *
   * @return RollbackResponse
   */
  public function rollback(RollbackRequest $request)
  {
    $response = new RollbackResponse();
    $this->_callMethod('rollback', $request, $response);
    return $response;
  }

  /**
   * @param RunQueryRequest $request
   *
   * @return RunQueryResponse
   */
  public function runQuery(RunQueryRequest $request)
  {
    $response = new RunQueryResponse();
    $this->_callMethod('runQuery', $request, $response);
    return $response;
  }
}
