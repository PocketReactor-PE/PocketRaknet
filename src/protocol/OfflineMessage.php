<?php

/*
  * RakLib network library
  *
  *
  * This project is not affiliated with Jenkins Software LLC nor RakNet.
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  */

namespace pocketraknet\protocol;

use pocketraknet\RakLib;

/**
 * Base class for offline messages (handshake and discovery).
 *
 * MAGIC authenticates nobody: it only tells a RakNet datagram apart from any other UDP
 * packet that lands on the port. That is what stops the server from answering garbage,
 * and therefore from acting as an amplification reflector.
 */

abstract class OfflineMessage extends Packet {

    /**
     * Smallest buffer that can be decoded without running past the end, ID byte included.
     * Checked BEFORE decode(): the readers in Packet are unbounded and raise PHP warnings
     * on a truncated buffer.
     */
    public static $MIN_LENGTH = 17; //1 (ID) + 16 (MAGIC)

    /** Filled by readMagic() when decoding; holds the expected MAGIC when encoding. */
    protected $magic = RakLib::MAGIC;

    protected function readMagic(){
        $this->magic = $this->get(16);
    }

    protected function writeMagic(){
        $this->put($this->magic);
    }

    public function isValid()
    {
        return $this->magic === RakLib::MAGIC;
    }
}