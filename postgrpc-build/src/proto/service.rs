use super::method::Method;
use prost_types::FileDescriptorSet;
use std::{
    collections::{hash_map::Values, HashMap},
    io,
};

/// [`prost`]-based Service type for handling groups of [`Method`]s
pub(crate) struct Service<'a> {
    methods: HashMap<String, Method<'a>>,
}

impl<'a> Service<'a> {
    /// Create a new [`Service`] from a map of [`Method`]s
    fn new(methods: HashMap<String, Method<'a>>) -> Self {
        Self { methods }
    }

    /// Get an Iterator over the methods in this service
    pub(crate) fn methods(&'a self) -> Values<'a, String, Method<'a>> {
        self.methods.values()
    }

    /// Get a single [`Method`] by name
    pub(crate) fn get_method(&'a self, name: &str) -> Option<&'a Method<'a>> {
        self.methods.get(name)
    }
}

/// Collection of [`prost`]-based Service types parsed from a [`prost_types::FileDescriptorSet`]
#[ouroboros::self_referencing]
pub(crate) struct Services {
    file_descriptor_set: FileDescriptorSet,
    #[covariant]
    #[borrows(file_descriptor_set)]
    services: HashMap<String, io::Result<Service<'this>>>,
}

impl Services {
    /// Create a new set of fully-resolved Service types from a [`prost_types::FileDescriptorSet`]
    pub(crate) fn from_file_descriptor_set(
        file_descriptor_set: FileDescriptorSet,
    ) -> io::Result<Self> {
        let protos = ServicesBuilder {
            file_descriptor_set,
            services_builder: |file_descriptor_set: &FileDescriptorSet| {
                let mut services = HashMap::new();

                for file in &file_descriptor_set.file {
                    let package = file.package();

                    // group all messages across the file by full message path + name
                    let messages = file
                        .message_type
                        .iter()
                        .map(|message| (format!(".{package}.{}", message.name()), message))
                        .collect();

                    // group all enums across files by full enum path + name
                    let enums = file
                        .enum_type
                        .iter()
                        .map(|enum_type| (format!(".{package}.{}", enum_type.name()), enum_type))
                        .collect();

                    // group the services and methods across files by full service path + name
                    for service in file.service.iter() {
                        let resolved_service = service
                            .method
                            .iter()
                            .filter_map(|method| {
                                Method::from_method_descriptor(method, &messages, &enums)
                                    .map(|method| {
                                        method.map(|method| (method.name().to_owned(), method))
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()
                            .map(Service::new);

                        services.insert(format!(".{package}.{}", service.name()), resolved_service);
                    }
                }

                services
            },
        }
        .build();

        Ok(protos)
    }

    /// Get a single [`Service`] by name (if it exists), including handling any errors that may
    /// have arisen during that service's module resolution
    pub(crate) fn try_get<'a>(&'a self, name: &str) -> io::Result<Option<&'a Service<'a>>> {
        match self.borrow_services().get(name) {
            Some(Err(error)) => Err(io::Error::new(error.kind(), error.to_string())),
            Some(Ok(service)) => Ok(Some(service)),
            None => Ok(None),
        }
    }
}

impl<'a> IntoIterator for &'a Services {
    type Item = &'a io::Result<Service<'a>>;
    type IntoIter = Values<'a, String, io::Result<Service<'a>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.borrow_services().values()
    }
}
