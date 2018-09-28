pub extern crate linkerd2_stack as stack;
extern crate tower_service;

pub use self::tower_service::{NewService, Service};

pub use self::stack::{
    make_per_request,
    watch,
    Either,
    Layer,
    Make,
    Shared,
};
