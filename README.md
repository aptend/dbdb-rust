DBDB in Rust, inspired by [500lines/dbdb](https://github.com/aosabook/500lines/tree/master/data-store/code/dbdb)


### RAII模式的文件锁

- 如何处理`FileStorage`中的`File`字段加文件锁
- 为`FileStorage`实现`cluFlock::FromElement`和`Deref`
- lock对象是`&mut File`，在使用时出现自引用问题
- `File::try_clone()`

### 递归类型定义

- 树节点和代理节点的递归
- `Rc<RefCell<T>>`的使用



### 类型参数(type parameter)和关联类型(associated type)该怎么选？

关联类型更强调与trait的实现类型的唯一对应。实现类型一旦确定，关联类型就不要改变了。如果使用类型参数，那么对同一个实现类型，可以用impl语句多次实现。


但也不能说实现类型只有一个关联类型。比如类型参数和关联类型同时使用时，
```rust
trait Foo<T> {
    type Inner;
    fn foo(&self, t: T) -> Self::Inner;
}

struct F;

impl Foo<i32> for F {
    type Inner = i32;
    fn foo(&self, t: i32) -> Self::Inner {
        t + 42
    }
}


impl Foo<String> for F {
    type Inner = String;
    fn foo(&self, t: String) -> Self::Inner {
        format!("String:{:?} 42", t)
    }
}

fn main() {
    let f = F;
    println!("{}", f.foo("answer".to_owned()));
    println!("{}", f.foo(12));
}

```

一般来说先从关联类型入手，当需要更高的灵活性是采用类型参数

