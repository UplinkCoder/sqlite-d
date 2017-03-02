module varint;
import utils;

//          Copyright Stefan Koch 2015 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE.md or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

enum unrolled = true;

struct VarInt
{
pure nothrow @safe @nogc:
    this(BigEndian!long value)
    {
        //FIXME the constructor does not work for value bigger then uint.max;
        auto len = lengthInVarInt(value);
        ubyte[9] tmp;
        long beValue = value.asBigEndian;

        auto _len = len;
        while (_len--)
        {
            tmp[_len] = (beValue & 0x7f) | 0x80;
            beValue >>= 7;
        }
        tmp[len - 1] &= 0x7f;

        byteArray = tmp[0 .. len];
    }

    const ubyte[] byteArray;

    alias toBeLong this;

    //TODO FIXME toBeLong does not correctly convert negative Numbers

    @property BigEndian!long toBeLong() @trusted
    {
        long tmp;
        static if (unrolled)
        {
            uint v3 = 0;

            if (byteArray.ptr[0] & 0x80)
            {
                v3 = 1;
                if (byteArray.ptr[1] & 0x80)
                {
                    v3 = 2;
                    if (byteArray.ptr[2] & 0x80)
                    {
                        v3 = 3;
                        if (byteArray.ptr[3] & 0x80)
                        {
                            v3 = 4;
                            if (byteArray.ptr[4] & 0x80)
                            {
                                v3 = 5;
                                if (byteArray.ptr[5] & 0x80)
                                {
                                    v3 = 6;
                                    if (byteArray.ptr[6] & 0x80)
                                    {
                                        v3 = 7;
                                        if (byteArray.ptr[7] & 0x80)
                                            v3 = 8;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                BigEndian!long result;

                version (LittleEndian)
                {
                    result.asNative = (cast(long) byteArray.ptr[0] << 56);
                }
                else
                {
                    result.asNative = (cast(long) byteArray.ptr[0]);
                }
                return result;
            }

        }
        else
        {
            uint length = length();
        }

        BigEndian!long result;

        // The hottest loop in the whole program!
        //TODO There must be a way to speed it up!
        static if (unrolled)
        {
            switch (v3)
            {
            case 8:
                tmp |= ((cast(long) byteArray.ptr[8]) << 7 * (v3 - 8));
                goto case 7;
            case 7:
                tmp |= ((cast(long) byteArray.ptr[7] & 0x7FUL) << 7 * (v3 - 7));
                goto case 6;
            case 6:
                tmp |= ((cast(long) byteArray.ptr[6] & 0x7FUL) << 7 * (v3 - 6));
                goto case 5;
            case 5:
                tmp |= ((cast(long) byteArray.ptr[5] & 0x7FUL) << 7 * (v3 - 5));
                goto case 4;
            case 4:
                tmp |= ((cast(long) byteArray.ptr[4] & 0x7FUL) << 7 * (v3 - 4));
                goto case 3;
            case 3:
                tmp |= ((cast(long) byteArray.ptr[3] & 0x7FUL) << 7 * (v3 - 3));
                goto case 2;
            case 2:
                tmp |= ((cast(long) byteArray.ptr[2] & 0x7FUL) << 7 * (v3 - 2));
                goto case 1;
            case 1:
                tmp |= ((cast(long) byteArray.ptr[1] & 0x7FUL) << 7 * (v3 - 1));
                tmp |= ((cast(long) byteArray.ptr[0] & 0x7FUL) << 7 * (v3 - 0));
                break;
            default:
                assert(0);

            }
        }
        else

        {
            foreach (idx; 0 .. length)
            {
                ubyte val = byteArray[idx];
                long maskedVal = (cast(long) val & 0x7fUL); // mask 8th bit
                long shiftBy = (length - idx - 1UL) * 7UL;
                if (idx < 8)
                {
                    tmp |= (maskedVal << shiftBy);
                }
                else
                {
                    tmp |= (cast(long) val << 63UL);
                }
            }
        }
        //this evokes swapIfNeeded
        result = tmp;

        return result;
    }

    this(const ubyte[] _arr)
    {
        this.byteArray = _arr;
    }

    static int lengthInVarInt(BigEndian!long value)
    {
        if (value > 1L << 56 || value < 0)
        {
            return 9;
        }
        else if (value < 1 << 7)
        {
            return 1;
        }
        else if (value < 1 << 14)
        {
            return 2;
        }
        else if (value < 1 << 21)
        {
            return 3;
        }
        else if (value < 1 << 28)
        {
            return 4;
        }
        else if (value < 1L << 35)
        {
            return 5;
        }
        else if (value < 1L << 42)
        {
            return 6;
        }
        else if (value < 1L << 49)
        {
            return 7;
        }
        else if (value < 1L << 56)
        {
            return 8;
        }
        assert(0, "We should never get here");
    }

    @property uint length()
    {
        return _length(byteArray);
    }

    static uint _length(const ubyte[] arr)
    {

        foreach (idx; 0 .. 9)
        {
            if (arr[idx] & (1 << 7))
            {
                continue;
            }
            else
            {
                return idx + 1;
            }
            assert(0, "we should never get here");
        }
        return 9;
    }

    static assert(_length((cast(ubyte[])[0x6d, 0x00])) == 1);
    static assert(_length((cast(ubyte[])[0x7f, 0x00])) == 1);
    static assert(_length((cast(ubyte[])[0x82, 0x12])) == 2);
    static assert(_length((cast(ubyte[])[0xfe, 0xfe, 0x00])) == 3);
    static assert(VarInt((cast(ubyte[])[0x81, 0x01])).toBeLong == 129);
    static assert(VarInt((cast(ubyte[])[0x81, 0x00])).toBeLong == 0x0080);
    static assert(VarInt((cast(ubyte[])[0x82, 0x00])).toBeLong == 0x0100); // should be 0x0100
    static assert(_length((cast(ubyte[])[0x82, 0x80, 0x00])) == 3);
    static assert(VarInt((cast(ubyte[])[0x84, 0x60, 0x00])).toBeLong == 608);
    //FIXME make this work!
    //	static assert(VarInt(cast(ubyte[])[0xFF, 0xFF, 0xFF, 0XFF, 0xFF, 0XFF, 0xFF, 0xFF, 0xEA]).toBeLong == -22);
    static assert(VarInt(bigEndian!long(265)).toBeLong == 265);
    static assert(VarInt(bigEndian!long(6421)).toBeLong == 6421);
    static assert(VarInt(bigEndian!long(22)).toBeLong == 22);
    static assert(VarInt.lengthInVarInt(BigEndian!long(-22)) == 9);
    static assert(VarInt(bigEndian!long(uint.max)).toBeLong == uint.max);
    static assert(VarInt(cast(ubyte[])[0xFF, 0xFF, 0xFF, 0XFF, 0xFF, 0XFF,
        0xFF, 0xFF, 0xEA]) == VarInt(bigEndian(-22L)));
    //static assert (VarInt().lengthInVarInt(608) == 2);
    static assert(VarInt((cast(ubyte[])[0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,
        0x89])).toBeLong != 0);
}
