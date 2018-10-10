package transaction;

import java.util.Objects;

final class Key {
    final int a;
    final int b;
    final int c;

    Key(int a, int b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Key)) {
            return false;
        }
        Key that = (Key) obj;
        return (this.a == that.a)
                && (this.b == that.b)
                && (this.c == that.c);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.a, this.b, this.c);
    }

}
