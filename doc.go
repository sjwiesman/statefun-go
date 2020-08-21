// Stateful Functions Go SDK
//
// Stateful Functions is an API that simplifies the building of **distributed stateful applications** with
// a **runtime built for serverless architectures**. It brings together the benefits of stateful stream
// processing, the processing of large datasets with low latency and bounded resource constraints,
// along with a runtime for modeling stateful entities that supports location transparency, concurrency,
// scaling, and resiliency.
//
// It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms
// like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless
// experience and elasticity of these platforms.
//
// Background
//
// The JVM-based Stateful Functions implementation has a RequestReply extension
// (a protocol and an implementation) that allows calling into any HTTP endpoint
// that implements that protocol. Although it is possible to implement this protocol
// independently, this is a minimal library for the Go programing language that:
//
// - Allows users to define and declare their functions in a convenient way.
//
// - Dispatches an invocation request sent from the JVM to the appropriate function previously declared.
package statefun_go
