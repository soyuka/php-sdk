<?php

/*
 * This file is part of the official PHP MCP SDK.
 *
 * A collaboration between Symfony and the PHP Foundation.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Mcp\Server\Transport;

use Mcp\Schema\JsonRpc\Error;
use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * @implements TransportInterface<ResponseInterface>
 */
class SseHttpTransport implements TransportInterface
{
    private StreamableHttpTransport $decorated;
    private ?LoggerInterface $logger;
    /** @var resource|null */
    private $writeStream = null;
    /** @var string[] */
    private array $outgoingMessages = [];
    /** @var array<string, string> */
    private array $corsHeaders = [
        'Access-Control-Allow-Origin' => '*',
        'Access-Control-Allow-Methods' => 'GET, POST, DELETE, OPTIONS',
        'Access-Control-Allow-Headers' => 'Content-Type, Mcp-Session-Id, Last-Event-ID, Authorization, Accept',
    ];

    public function __construct(
        private readonly ServerRequestInterface $request,
        private readonly ResponseFactoryInterface $responseFactory,
        private readonly StreamFactoryInterface $streamFactory,
        LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->decorated = new StreamableHttpTransport($request, $responseFactory, $streamFactory, $this->logger);
    }

    public function listen(): ResponseInterface
    {
        if ($this->request->getMethod() === 'GET' && str_contains($this->request->getHeaderLine('Accept'), 'text/event-stream')) {
            return $this->handleGetRequest();
        }

        return $this->decorated->listen();
    }

    public function send(string $data, array $context): void
    {
        if ($this->writeStream) {
            $this->logger->debug('Sending SSE data', ['data' => $data]);
            fwrite($this->writeStream, "data: {$data}\n\n");

            return;
        }

        if ($this->request->getMethod() === 'GET' && str_contains($this->request->getHeaderLine('Accept'), 'text/event-stream')) {
            // Buffer messages for the SSE stream
            $this->outgoingMessages[] = $data;

            return;
        }

        $this->decorated->send($data, $context);
    }

    public function initialize(): void
    {
        $this->decorated->initialize();
    }

    public function onMessage(callable $listener): void
    {
        $this->decorated->onMessage($listener);
    }

    public function onSessionEnd(callable $listener): void
    {
        $this->decorated->onSessionEnd($listener);
    }

    public function close(): void
    {
        if ($this->writeStream) {
            fclose($this->writeStream);
        }
        $this->decorated->close();
    }

    private function handleGetRequest(): ResponseInterface
    {
        // Create a pipe for streaming
        $pipes = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if (false === $pipes) {
            $this->logger->error('Failed to create stream socket pair.');

            return $this->createErrorResponse(Error::forInternalError('Failed to create stream.'), 500);
        }
        [$readStream, $this->writeStream] = $pipes;

        // Make read stream non-blocking to avoid deadlocks if the buffer is full on some systems
        stream_set_blocking($readStream, false);

        $response = $this->responseFactory->createResponse(200)
            ->withHeader('Content-Type', 'text/event-stream')
            ->withHeader('Cache-Control', 'no-cache')
            ->withHeader('Connection', 'keep-alive')
            ->withHeader('X-Accel-Buffering', 'no')
            ->withBody($this->streamFactory->createStreamFromResource($readStream));

        // Send any buffered messages
        foreach ($this->outgoingMessages as $message) {
            fwrite($this->writeStream, "data: {$message}\n\n");
        }
        $this->outgoingMessages = [];

        return $this->withCorsHeaders($response);
    }

    private function withCorsHeaders(ResponseInterface $response): ResponseInterface
    {
        foreach ($this->corsHeaders as $name => $value) {
            $response = $response->withHeader($name, $value);
        }

        return $response;
    }

    private function createErrorResponse(Error $jsonRpcError, int $statusCode): ResponseInterface
    {
        $errorPayload = json_encode($jsonRpcError, \JSON_THROW_ON_ERROR);

        return $this->responseFactory->createResponse($statusCode)
            ->withHeader('Content-Type', 'application/json')
            ->withBody($this->streamFactory->createStream($errorPayload));
    }
}
