# Go Coding Guidelines

## Interface Compliance
Whenever creating or modifying Go structs intended to implement interfaces, always include compile-time interface compliance assertions per the [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md#verify-interface-compliance):

```go
var _ InterfaceType = (*ConcreteType)(nil)
```
