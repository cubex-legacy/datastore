<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore\GoogleAPI;

use Cubex\Facade\Cache;
use DrSlump\Protobuf\Message;

abstract class GoogleService
{
  /**
   * @var GoogleServiceOptions
   */
  protected $_options;

  /**
   * How many times to retry a request that returns a 503 error
   * @var int
   */
  protected $_defaultRetryCount = 3;
  /**
   * How long in seconds to sleep for each retry. This is multiplied by the
   * number of retries so the delay gets longer on each attempt.
   * @var int
   */
  protected $_retrySleepFactor = 2;

  public function __construct(GoogleServiceOptions $options = null)
  {
    $this->_options = $options;
  }

  /**
   * @return string
   */
  abstract protected function _getBaseUrl();
  /**
   * @param string $methodName
   *
   * @return string
   */
  abstract protected function _getUrlForMethod($methodName);
  /**
   * @return string[]
   */
  abstract protected function _getOAuthScopes();

  protected function _getFullBaseUrl()
  {
    return rtrim($this->_options->host, '/') . '/' .
    ltrim($this->_getBaseUrl(), '/');
  }


  /**
   * @param string  $methodName
   * @param Message $message
   * @param Message $response
   * @param int     $retryCount How many times to attempt the call.
   *                            -1 = use default.
   *
   * @return Message
   * @throws GoogleServiceException
   */
  protected function _callMethod(
    $methodName, Message $message, Message $response, $retryCount = -1
  )
  {
    if($retryCount == -1)
    {
      $retryCount = $this->_defaultRetryCount;
    }

    $url = $this->_getUrlForMethod($methodName);
    $client = $this->_getClient();

    $postBody = $message->serialize();

    $headers = [
      'Accept-Encoding' => 'gzip',
      'Accept' => 'text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2',
      'Content-Type' => 'application/x-protobuf',
      'Content-Length' => strlen($postBody)
    ];

    $finished = false;
    $numTries = 1;
    while(! $finished)
    {
      $httpRequest = new \Google_HttpRequest($url, 'POST', $headers, $postBody);
      \Google_Client::$auth->sign($httpRequest);

      $httpResponse = \Google_Client::$io->makeRequest($httpRequest);

      $code = $httpResponse->getResponseHttpCode();
      if($code == 200)
      {
        $response->parse($httpResponse->getResponseBody());
        $finished = true;
      }
      else if(($code == 503) && ($numTries < $retryCount))
      {
        sleep($numTries * $this->_retrySleepFactor);
        $numTries++;
      }
      else
      {
        $msg = "ERROR: HTTP request returned code " . $code . "\n" .
          $httpResponse->getResponseBody();
        throw new GoogleServiceException($msg, $code);
      }
    }

    $token = $client->getAccessToken();
    if($token)
    {
      $this->_saveAuthToken($token);
    }
    return $response;
  }

  protected function _getClient()
  {
    $client = new \Google_Client();
    $client->setApplicationName($this->_options->applicationName);

    if($this->_options->serviceAccountName)
    {
      $authToken = $this->_getAuthToken();
      if($authToken)
      {
        $client->setAccessToken($authToken);
      }

      $client->setAssertionCredentials(
        new \Google_AssertionCredentials(
          $this->_options->serviceAccountName,
          $this->_getOAuthScopes(),
          $this->_getPrivateKey()
        )
      );
    }
    if($this->_options->clientId)
    {
      $client->setClientId($this->_options->clientId);
    }

    if(! $this->_options->getAllowIPv6())
    {
      $client->getIo()->setOptions([CURLOPT_IPRESOLVE => CURL_IPRESOLVE_V4]);
    }

    return $client;
  }

  protected function _getPrivateKey()
  {
    $file = $this->_options->privateKeyFile;
    if(! file_exists($file))
    {
      echo "ERROR: Could not find private key file: " . $file . "\n";
      die;
    }
    return file_get_contents($file);
  }

  protected function _getAuthToken()
  {
    if($this->_options->authTokenInMemcache)
    {
      return Cache::get($this->_authTokenCacheKey(), false);
    }
    else if($this->_options->authTokenFile != "")
    {
      return file_exists($this->_options->authTokenFile) ?
        file_get_contents($this->_options->authTokenFile) : false;
    }
    else
    {
      return false;
    }
  }

  protected function _saveAuthToken($token)
  {
    if($this->_options->authTokenInMemcache)
    {
      Cache::set($this->_authTokenCacheKey(), $token, 3600);
    }
    else if($this->_options->authTokenFile != "")
    {
      file_put_contents($this->_options->authTokenFile, $token);
    }
  }

  protected function _authTokenCacheKey()
  {
    return 'GoogleService:authToken:' .
      $this->_options->getServiceAccountName();
  }
}
