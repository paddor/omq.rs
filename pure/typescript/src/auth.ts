/** Username/password credentials for the ZMTP PLAIN mechanism. */
export interface PlainAuthOptions {
  /** Username sent during the PLAIN handshake. UTF-8 encoded, max 255 bytes. */
  username: string;
  /** Password sent during the PLAIN handshake. UTF-8 encoded, max 255 bytes. */
  password: string;
}
