<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore\GoogleAPI;

class GoogleServiceOptions
{
  public $host = 'https://www.googleapis.com';
  public $applicationName = '';
  public $clientId = '';
  public $serviceAccountName = '';
  public $privateKeyFile;
  public $privateKeyPassword = 'notasecret';
  public $authTokenInMemcache = false;
  public $authTokenFile;
  public $allowIPv6 = false;

  public function getHost()
  {
    return $this->host;
  }

  public function setHost($host)
  {
    $this->host = $host;
    return $this;
  }

  public function getApplicationName()
  {
    return $this->applicationName;
  }

  public function setApplicationName($applicationName)
  {
    $this->applicationName = $applicationName;
    return $this;
  }

  public function getClientId()
  {
    return $this->clientId;
  }

  public function setClientId($clientId)
  {
    $this->clientId = $clientId;
    return $this;
  }

  public function getServiceAccountName()
  {
    return $this->serviceAccountName;
  }

  public function setServiceAccountName($serviceAccountName)
  {
    $this->serviceAccountName = $serviceAccountName;
    return $this;
  }

  public function getPrivateKeyFile()
  {
    return $this->privateKeyFile;
  }

  public function setPrivateKeyFile($privateKeyFile)
  {
    $this->privateKeyFile = $privateKeyFile;
    return $this;
  }

  public function setPrivateKeyPassword($password)
  {
    $this->privateKeyPassword = $password;
    return $this;
  }

  public function getAuthTokenFile()
  {
    return $this->authTokenFile;
  }

  public function setAuthTokenFile($authTokenFile)
  {
    $this->authTokenFile = $authTokenFile;
    return $this;
  }

  public function getAuthTokenInMemcache()
  {
    return $this->authTokenInMemcache;
  }

  public function setAuthTokenInMemcache($authTokenInMemcache)
  {
    $this->authTokenInMemcache = $authTokenInMemcache;
    return $this;
  }

  public function getAllowIPv6()
  {
    return $this->allowIPv6;
  }

  public function setAllowIPv6($allowIPv6)
  {
    $this->allowIPv6 = $allowIPv6;
    return $this;
  }
}
