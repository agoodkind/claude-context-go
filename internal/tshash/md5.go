// Package tshash produces the routing-key hash that the upstream
// TypeScript adapter uses to name Milvus collections. Drop-in compatibility
// requires byte-exact MD5 output. The implementation is a direct port of
// RFC 1321 so the daemon does not depend on `crypto/md5`. The hash is a
// routing key, not a security primitive, and is never trusted.
package tshash

import "encoding/binary"

// PathPrefix returns the first 8 hex characters of MD5(input). The upstream
// TS adapter uses this exact value as the Milvus collection-name suffix at
// packages/core/src/context.ts:275.
func PathPrefix(input string) string {
	return FullHex(input)[:8]
}

// FullHex returns the full 32-character lowercase hex MD5 of input. The
// upstream TS adapter names its per-codebase merkle file
// ~/.context/merkle/<FullHex(path)>.json (packages/core/src/sync/synchronizer.ts
// getSnapshotPath), so the daemon uses this to locate that file for read-only
// adoption seeding.
func FullHex(input string) string {
	digest := sum([]byte(input))
	const hex = "0123456789abcdef"
	out := make([]byte, 32)
	for index := range 16 {
		out[index*2] = hex[digest[index]>>4]
		out[index*2+1] = hex[digest[index]&0x0f]
	}
	return string(out)
}

var md5K = [64]uint32{
	0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
	0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
	0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
	0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
	0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
	0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
	0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
	0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
	0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
	0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
	0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
	0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
	0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
	0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
	0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
	0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
}

var md5R = [64]uint8{
	7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
	5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
	4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
	6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
}

func sum(data []byte) [16]byte {
	state := [4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476}

	bitLength := uint64(len(data)) * 8
	padded := make([]byte, 0, len(data)+72)
	padded = append(padded, data...)
	padded = append(padded, 0x80)
	for len(padded)%64 != 56 {
		padded = append(padded, 0x00)
	}
	var lengthBuffer [8]byte
	binary.LittleEndian.PutUint64(lengthBuffer[:], bitLength)
	padded = append(padded, lengthBuffer[:]...)

	for offset := 0; offset < len(padded); offset += 64 {
		var message [16]uint32
		for index := range 16 {
			message[index] = binary.LittleEndian.Uint32(padded[offset+index*4:])
		}
		a, b, c, d := state[0], state[1], state[2], state[3]
		for index := range 64 {
			var function uint32
			var gather int
			switch {
			case index < 16:
				function = (b & c) | (^b & d)
				gather = index
			case index < 32:
				function = (d & b) | (^d & c)
				gather = (5*index + 1) % 16
			case index < 48:
				function = b ^ c ^ d
				gather = (3*index + 5) % 16
			default:
				function = c ^ (b | ^d)
				gather = (7 * index) % 16
			}
			temp := d
			d = c
			c = b
			b += leftRotate(a+function+md5K[index]+message[gather], md5R[index])
			a = temp
		}
		state[0] += a
		state[1] += b
		state[2] += c
		state[3] += d
	}

	var digest [16]byte
	binary.LittleEndian.PutUint32(digest[0:4], state[0])
	binary.LittleEndian.PutUint32(digest[4:8], state[1])
	binary.LittleEndian.PutUint32(digest[8:12], state[2])
	binary.LittleEndian.PutUint32(digest[12:16], state[3])
	return digest
}

func leftRotate(value uint32, count uint8) uint32 {
	return (value << count) | (value >> (32 - count))
}
