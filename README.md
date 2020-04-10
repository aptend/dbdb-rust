DBDB in Rust, inspired by [500lines/dbdb](https://github.com/aosabook/500lines/DBTree/master/data-store/code/dbdb)


### RAII模式的文件锁

- 如何处理`FileStorage`中的`File`字段加文件锁
- 为`FileStorage`实现`cluFlock::FromElement`和`Deref`
- 如果lock对象是`&mut File`，在使用时出现自引用问题, 所以使用了`File::try_clone()`
- 跨多个调用，需要维护`Guard`变量，但不使用，合适吗？

### 递归类型定义

- 树节点

  `Rc<RefCell<T>>`就可以看成是Python的变量名，采用引用计数回收内存

- 代理节点的递归

    打算给Agent写一个trait，`Agent<T>`，然后每个`TreeNode`的左右子节点指向`Agent`的泛型，使用时用`TreeNode<V, N>`确定`TreeNode`的具体类型，这样就可以设置不同的`V`和`N`，得到不同行为的`store`、`get`、`get_mut`

- 利用`DBTree` trait分离具体的树实现，`LogicalTree`只用来管理`Storage`的并发，读写请求都直接转给`DBTree`

    但是，`DBTree`工作需要传入`Storage`的独占引用，`DBTree`本身也要mut，就给`LogicalTree`造成了所有权的冲突。解决选择：1. 把`Storage` Rc化. 2. `Storage`放入`DBTree`，直接利用本身已经Rc的`NodeAgentCell`。第二种实际就是未设计`DBTree`时的方案，每次操作只会使用`Storage`的独占引用，`DBTree`则由`NodeAgentCell`作为根节点来表示，每次操作克隆一份，用完直接销毁就好。因此，似乎这里应该选择第一种方案，`DBTree`借出独占引用，然后使用共享所有权的`Storage`。

    但两个方式都没有逃过Rc的范围，那这种两个mut对象的协作的场景有更好的解决方案吗？

- 共享`Storage`后，运行时显示`BorrowMutError`，重复借出独占引用，调用栈定位到`put`方法：
    ```rust
    if self.guard.is_none() {
        self.begin()?;
        let storage = self.storage.clone();
        let storage = &mut *storage.borrow_mut();
        self.tree.insert(key, value, storage)?;
        self.commit()?;
    }
        ...
   
    ```
    确实`commit`中也向`storage`索要了独占引用。第一反应是`insert`之后`drop(storage);`，不过错误依旧。想起来借用检查是和作用域相关的，所以改成下面这样就没问题了

    ```rust
    if self.guard.is_none() {
        self.begin()?;
        {
            let storage = self.storage.clone();
            let storage = &mut *storage.borrow_mut();
            self.tree.insert(key, value, storage)?;
        }
        self.commit()?;
    }
        ...
    ```
    记下来作为之后理解`drop`和运行时检查的材料




### 类型参数(type parameter)和关联类型(associated type)该怎么选？

关联类型更强调与trait的实现类型的唯一对应。实现类型一旦确定，关联类型就不要改变了。如果使用类型参数，那么对同一个实现类型，可以用impl语句多次实现。


但也不能说实现类型只有一个关联类型。比如类型参数和关联类型同时使用时。
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

一般来说先从关联类型入手，当需要更高的灵活性时采用类型参数
关联类型的Trait绑定还是还在proposal阶段？


### 树的大小

处理 `insert` 和 `delete` 时不能采用 `node.size = node.left.size + node.right.size` 的方式来更新`size`，因为访问查找路径外的`size`字段都会引起一次多余的IO，太不划算。所以给`insert` `delete`的返回值加一个`usize`，指示是否真的插入或删除了一个节点，方便修改当前节点的`size`

当然`delta_size`可以通过先`find`一次来去掉。

这个方式的成本在于，每次会多一次遍历。但是好处是，比如，如果删除不存在的key，先`find`就不会产生`delete`，也就不会修改树，`commit`时就不会有真正的写入发生。`insert`时也可以通过检查，避免相同值导致的树落盘。

这样一考虑，应该先`find`。其实多一次的遍历也完全发生在内存中，会比较快，相比IO的消耗(即使顺序写)，这个成本也小一些。
