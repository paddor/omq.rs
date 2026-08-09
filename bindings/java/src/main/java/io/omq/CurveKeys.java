package io.omq;

final class CurveKeys {
    private static final String Z85_ALPHABET =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    private static final boolean[] Z85_CHARS = z85Chars();

    private CurveKeys() {
    }

    static void requireZ85Key(String name, String value) {
        if (value.length() != 40) {
            throw new IllegalArgumentException(name + " must be 40 Z85 characters");
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch >= Z85_CHARS.length || !Z85_CHARS[ch]) {
                throw new IllegalArgumentException(name + " contains a non-Z85 character");
            }
        }
    }

    private static boolean[] z85Chars() {
        boolean[] chars = new boolean[128];
        for (int i = 0; i < Z85_ALPHABET.length(); i++) {
            chars[Z85_ALPHABET.charAt(i)] = true;
        }
        return chars;
    }
}
