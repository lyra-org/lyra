// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod compile;
mod load;

extern crate proc_macro;

use crate::compile::parse_compile_args;

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::{
    Ident,
    Span,
};
use quote::{
    format_ident,
    quote,
};
use syn::{
    Attribute,
    Expr,
    ExprLit,
    Fields,
    FnArg,
    ImplItem,
    Lit,
    LitStr,
    Meta,
    Pat,
    ReturnType,
    Token,
    Type,
    TypePath,
    TypeReference,
    parse::{
        Parse,
        ParseStream,
    },
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
};

#[derive(Debug, Default, FromMeta)]
struct ModuleScalarOptions {
    #[darling(default)]
    name: Option<String>,
    #[darling(default)]
    local: Option<String>,
    #[darling(default)]
    path: Option<String>,
    #[darling(default)]
    plugin_scoped: bool,
}

#[derive(Debug, Default)]
struct ModuleOptions {
    name: Option<String>,
    local: Option<String>,
    path: Option<String>,
    aliases: Vec<TypePath>,
    interfaces: Vec<TypePath>,
    classes: Vec<TypePath>,
    plugin_scoped: bool,
}

#[derive(Default)]
struct DocInfo {
    description: Option<String>,
}

#[derive(Default)]
struct HarmonyFnOptions {
    path: Option<String>,
    skip: bool,
    args: Option<Vec<DocParameterOverride>>,
    returns: Option<Vec<Type>>,
}

struct DocParameterOverride {
    name: String,
    ty: Type,
}

struct DocParameterList(Punctuated<DocParameter, Token![,]>);

struct DocParameter {
    name: Ident,
    _colon: Token![:],
    ty: Type,
}

struct DocReturnList(Punctuated<Type, Token![,]>);
enum HarmonyFnOption {
    Path(LitStr),
    Skip,
    Args(DocParameterList),
    Returns(DocReturnList),
}

struct HarmonyFnOptionList(Punctuated<HarmonyFnOption, Token![,]>);

struct TypePathList(Punctuated<TypePath, Token![,]>);

impl Parse for DocParameter {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _colon: input.parse()?,
            ty: input.parse()?,
        })
    }
}

impl Parse for DocParameterList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for DocReturnList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for HarmonyFnOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;

        if ident == "skip" {
            return Ok(Self::Skip);
        }

        if ident == "path" {
            let _: Token![=] = input.parse()?;
            return Ok(Self::Path(input.parse()?));
        }

        if ident == "args" {
            let content;
            syn::parenthesized!(content in input);
            return Ok(Self::Args(content.parse()?));
        }

        if ident == "returns" {
            let content;
            syn::parenthesized!(content in input);
            return Ok(Self::Returns(content.parse()?));
        }

        Err(syn::Error::new(
            ident.span(),
            "unsupported harmony option; expected path, skip, args, or returns",
        ))
    }
}

impl Parse for HarmonyFnOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for TypePathList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

#[proc_macro_attribute]
pub fn structure(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast: syn::ItemStruct = parse_macro_input!(item as syn::ItemStruct);
    let name = &ast.ident;

    let description = match extract_doc_info(&ast.attrs) {
        Ok(info) => info,
        Err(error) => return error.into_compile_error().into(),
    };
    let description_expr = option_str_tokens(description.description.as_deref());
    let mut user_data_fields = Vec::new();
    let mut field_descriptors = Vec::new();

    for field in &ast.fields {
        let field_name = match &field.ident {
            Some(name) => name,
            None => {
                return syn::Error::new(field.span(), "structure fields must be named")
                    .into_compile_error()
                    .into();
            }
        };
        let field_name_str = lit_str(&field_name.to_string(), field_name.span());
        let field_ty = &field.ty;
        let field_info = match extract_doc_info(&field.attrs) {
            Ok(info) => info,
            Err(error) => return error.into_compile_error().into(),
        };
        let field_description = option_str_tokens(field_info.description.as_deref());

        user_data_fields.push(quote! {
            fields.add_field_method_get(#field_name_str, |_, this| {
                Ok(this.#field_name.clone())
            });
        });

        user_data_fields.push(quote! {
            fields.add_field_method_set(#field_name_str, |_, this, val: #field_ty| {
                this.#field_name = val;
                Ok(())
            });
        });

        field_descriptors.push(quote! {
            ::harmony_luau::FieldDescriptor {
                name: #field_name_str,
                ty: <#field_ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                description: #field_description,
            }
        });
    }

    let helper_tokens = quote! {
        #[automatically_derived]
        impl #name {
            #[doc(hidden)]
            pub fn _harmony_userdata_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
                #(#user_data_fields)*
            }

            #[doc(hidden)]
            pub fn _harmony_luau_fields() -> ::std::vec::Vec<::harmony_luau::FieldDescriptor> {
                vec![#(#field_descriptors),*]
            }

            #[doc(hidden)]
            pub fn _harmony_luau_description() -> ::std::option::Option<&'static str> {
                #description_expr
            }
        }
    };

    quote! {
        #ast
        #helper_tokens
    }
    .into()
}

#[proc_macro_attribute]
pub fn interface(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast: syn::ItemStruct = parse_macro_input!(item as syn::ItemStruct);
    let name = &ast.ident;

    let description = match extract_doc_info(&ast.attrs) {
        Ok(info) => info,
        Err(error) => return error.into_compile_error().into(),
    };
    let description_expr = option_str_tokens(description.description.as_deref());
    let mut field_descriptors = Vec::new();
    let mut field_usage = Vec::new();

    for field in &ast.fields {
        let field_name = match &field.ident {
            Some(name) => name,
            None => {
                return syn::Error::new(field.span(), "interface fields must be named")
                    .into_compile_error()
                    .into();
            }
        };
        let field_name_str = lit_str(&field_name.to_string(), field_name.span());
        let field_ty = &field.ty;
        let field_info = match extract_doc_info(&field.attrs) {
            Ok(info) => info,
            Err(error) => return error.into_compile_error().into(),
        };
        let field_description = option_str_tokens(field_info.description.as_deref());

        field_usage.push(quote! {
            let _ = &value.#field_name;
        });

        field_descriptors.push(quote! {
            ::harmony_luau::FieldDescriptor {
                name: #field_name_str,
                ty: <#field_ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                description: #field_description,
            }
        });
    }

    let type_name_lit = lit_str(&name.to_string(), name.span());

    quote! {
        #ast

        #[automatically_derived]
        impl ::harmony_luau::LuauTypeInfo for #name {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(#type_name_lit)
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeInterface for #name {
            fn interface_descriptor() -> ::harmony_luau::InterfaceDescriptor {
                let mut descriptor = ::harmony_luau::InterfaceDescriptor::new(
                    #type_name_lit,
                    #description_expr,
                );
                descriptor.fields.extend(vec![#(#field_descriptors),*]);
                descriptor
            }
        }

        #[doc(hidden)]
        const _: fn(&#name) = |value| {
            #(#field_usage)*
        };
    }
    .into()
}

#[proc_macro_attribute]
pub fn enumeration(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast: syn::ItemEnum = parse_macro_input!(item as syn::ItemEnum);
    let name = &ast.ident;

    let mut variant_registrations = Vec::new();
    for variant in &ast.variants {
        let variant_name = &variant.ident;
        let variant_name_str = lit_str(&variant_name.to_string(), variant_name.span());

        match &variant.fields {
            Fields::Unit => {
                variant_registrations.push(quote! {
                    fields.add_field_function_get(#variant_name_str, |_, _| {
                        Ok(#name::#variant_name)
                    });
                });
            }
            Fields::Unnamed(_) => {
                return syn::Error::new_spanned(
                    variant,
                    "enumeration variants with unnamed fields are not supported",
                )
                .into_compile_error()
                .into();
            }
            Fields::Named(_) => {
                return syn::Error::new_spanned(
                    variant,
                    "enumeration variants with named fields are not supported",
                )
                .into_compile_error()
                .into();
            }
        }
    }

    let variant_names: Vec<_> = ast
        .variants
        .iter()
        .map(|v| lit_str(&v.ident.to_string(), v.ident.span()))
        .collect();
    let type_name_lit = lit_str(&name.to_string(), name.span());

    let helper_tokens = quote! {
        #[automatically_derived]
        impl #name {
            #[doc(hidden)]
            pub fn _harmony_userdata_variant_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
                #(#variant_registrations)*
            }

            #[doc(hidden)]
            pub fn _harmony_luau_variant_fields() -> Vec<::harmony_luau::FieldDescriptor> {
                vec![
                    #(::harmony_luau::FieldDescriptor {
                        name: #variant_names,
                        ty: ::harmony_luau::LuauType::literal(#type_name_lit),
                        description: None,
                    }),*
                ]
            }
        }
    };

    quote! {
        #ast
        #helper_tokens
    }
    .into()
}

fn parse_plugin_scoped_attr(attr: TokenStream) -> syn::Result<bool> {
    if attr.is_empty() {
        return Ok(false);
    }
    let ident: Ident = syn::parse(attr.clone())
        .map_err(|_| syn::Error::new(Span::call_site(), "expected identifier 'plugin_scoped'"))?;
    if ident == "plugin_scoped" {
        Ok(true)
    } else {
        Err(syn::Error::new(
            ident.span(),
            "unsupported option; only 'plugin_scoped' is accepted",
        ))
    }
}

#[proc_macro_attribute]
pub fn implementation(attr: TokenStream, item: TokenStream) -> TokenStream {
    let plugin_scoped = match parse_plugin_scoped_attr(attr) {
        Ok(flag) => flag,
        Err(error) => return error.into_compile_error().into(),
    };
    let mut ast: syn::ItemImpl = parse_macro_input!(item as syn::ItemImpl);
    let self_ty = &ast.self_ty;

    let mut method_registrations = Vec::new();
    let mut method_descriptors = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };

        let options = match parse_harmony_fn_options(&fn_item.attrs) {
            Ok(options) => options,
            Err(error) => return error.into_compile_error().into(),
        };
        strip_helper_attributes(&mut fn_item.attrs, "harmony");
        let doc_info = match extract_doc_info(&fn_item.attrs) {
            Ok(info) => info,
            Err(error) => return error.into_compile_error().into(),
        };
        if options.skip {
            continue;
        }

        let fn_name = &fn_item.sig.ident;
        let fn_name_str = lit_str(&fn_name.to_string(), fn_name.span());
        let description = option_str_tokens(doc_info.description.as_deref());
        let params = collect_parameters(&fn_item.sig.inputs);
        let param_names: Vec<_> = params.iter().map(|(name, _, _)| *name).collect();
        let param_types: Vec<_> = params.iter().map(|(_, ty, _)| *ty).collect();
        let returns_result = returns_result_type(&fn_item.sig);
        let param_descriptors =
            match parameter_descriptors(&options, &fn_item.sig.inputs, fn_item.sig.ident.span()) {
                Ok(descriptors) => descriptors,
                Err(error) => return error.into_compile_error().into(),
            };
        let return_tokens = match return_tokens_with_doc_info(&options, &fn_item.sig.output) {
            Ok(tokens) => tokens,
            Err(error) => return error.into_compile_error().into(),
        };
        let yields = fn_item.sig.asyncness.is_some();
        let yields_tokens = if yields {
            quote! { true }
        } else {
            quote! { false }
        };

        if let Some(receiver) = fn_item.sig.receiver() {
            let kind = quote! { ::harmony_luau::MethodKind::Instance };
            if receiver.mutability.is_some() {
                if yields {
                    let body = if returns_result {
                        quote! { Ok(this.#fn_name(#(#param_names,)*).await?) }
                    } else {
                        quote! { Ok(this.#fn_name(#(#param_names,)*).await) }
                    };
                    method_registrations.push(quote! {
                        methods.add_async_method_mut(#fn_name_str, |_, mut this, (#(#param_names,)*): (#(#param_types,)*)| async move {
                            #body
                        });
                    });
                } else {
                    let body = if returns_result {
                        quote! { Ok(this.#fn_name(#(#param_names,)*)?) }
                    } else {
                        quote! { Ok(this.#fn_name(#(#param_names,)*)) }
                    };
                    method_registrations.push(quote! {
                        methods.add_method_mut(
                            #fn_name_str,
                            |_, this, (#(#param_names,)*): (#(#param_types,)*)| {
                                #body
                            },
                        );
                    });
                }
            } else if yields {
                let registration = if plugin_scoped {
                    let body = if returns_result {
                        quote! { Ok(this.#fn_name(__harmony_plugin_id, #(#param_names,)*).await?) }
                    } else {
                        quote! { Ok(this.#fn_name(__harmony_plugin_id, #(#param_names,)*).await) }
                    };
                    quote! {
                        methods.add_async_method_with_prelude(
                            #fn_name_str,
                            |lua| ::harmony_core::resolve_caller(lua),
                            |_, __harmony_plugin_id, this, (#(#param_names,)*): (#(#param_types,)*)| async move {
                                #body
                            },
                        );
                    }
                } else {
                    let body = if returns_result {
                        quote! { Ok(this.#fn_name(#(#param_names,)*).await?) }
                    } else {
                        quote! { Ok(this.#fn_name(#(#param_names,)*).await) }
                    };
                    quote! {
                        methods.add_async_method(#fn_name_str, |_, this, (#(#param_names,)*): (#(#param_types,)*)| async move {
                            #body
                        });
                    }
                };
                method_registrations.push(registration);
            } else {
                let body = if returns_result {
                    quote! { Ok(this.#fn_name(#(#param_names,)*)?) }
                } else {
                    quote! { Ok(this.#fn_name(#(#param_names,)*)) }
                };
                method_registrations.push(quote! {
                    methods.add_method(
                        #fn_name_str,
                        |_, this, (#(#param_names,)*): (#(#param_types,)*)| {
                            #body
                        },
                    );
                });
            }

            method_descriptors.push(quote! {
                ::harmony_luau::MethodDescriptor {
                    name: #fn_name_str,
                    description: #description,
                    params: vec![#(#param_descriptors),*],
                    returns: #return_tokens,
                    yields: #yields_tokens,
                    kind: #kind,
                }
            });
        } else {
            if yields {
                let registration = if plugin_scoped {
                    let body = if returns_result {
                        quote! { Ok(<#self_ty>::#fn_name(__harmony_plugin_id, #(#param_names,)*).await?) }
                    } else {
                        quote! { Ok(<#self_ty>::#fn_name(__harmony_plugin_id, #(#param_names,)*).await) }
                    };
                    quote! {
                        methods.add_async_function_with_prelude(
                            #fn_name_str,
                            |lua| ::harmony_core::resolve_caller(lua),
                            |_, __harmony_plugin_id, (#(#param_names,)*): (#(#param_types,)*)| async move {
                                #body
                            },
                        );
                    }
                } else {
                    let body = if returns_result {
                        quote! { Ok(<#self_ty>::#fn_name(#(#param_names,)*).await?) }
                    } else {
                        quote! { Ok(<#self_ty>::#fn_name(#(#param_names,)*).await) }
                    };
                    quote! {
                        methods.add_async_function(#fn_name_str, |_, (#(#param_names,)*): (#(#param_types,)*)| async move {
                            #body
                        });
                    }
                };
                method_registrations.push(registration);
            } else {
                let body = if returns_result {
                    quote! { Ok(<#self_ty>::#fn_name(#(#param_names,)*)?) }
                } else {
                    quote! { Ok(<#self_ty>::#fn_name(#(#param_names,)*)) }
                };
                method_registrations.push(quote! {
                    methods.add_function(
                        #fn_name_str,
                        |_, (#(#param_names,)*): (#(#param_types,)*)| {
                            #body
                        },
                    );
                });
            }

            method_descriptors.push(quote! {
                ::harmony_luau::MethodDescriptor {
                    name: #fn_name_str,
                    description: #description,
                    params: vec![#(#param_descriptors),*],
                    returns: #return_tokens,
                    yields: #yields_tokens,
                    kind: ::harmony_luau::MethodKind::Static,
                }
            });
        }
    }

    let helper_tokens = quote! {
        #[automatically_derived]
        impl #self_ty {
            #[doc(hidden)]
            pub fn _harmony_userdata_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
                #(#method_registrations)*
            }

            #[doc(hidden)]
            pub fn _harmony_luau_methods() -> ::std::vec::Vec<::harmony_luau::MethodDescriptor> {
                vec![#(#method_descriptors),*]
            }
        }
    };

    quote! {
        #ast
        #helper_tokens
    }
    .into()
}

#[proc_macro_attribute]
pub fn module(attr: TokenStream, item: TokenStream) -> TokenStream {
    let module_options = match parse_module_options(attr) {
        Ok(options) => options,
        Err(error) => return error.into_compile_error().into(),
    };

    let mut ast: syn::ItemImpl = parse_macro_input!(item as syn::ItemImpl);
    let self_ty = &ast.self_ty;
    let self_name = type_name_from_type(self_ty);
    let module_name = module_options.name.unwrap_or_else(|| self_name.clone());
    let local_name = module_options
        .local
        .unwrap_or_else(|| default_module_local_name(&self_name));
    let module_path = module_options.path;
    let module_name_lit = lit_str(&module_name, Span::call_site());
    let local_name_lit = lit_str(&local_name, Span::call_site());
    let doc_info = match extract_doc_info(&ast.attrs) {
        Ok(info) => info,
        Err(error) => return error.into_compile_error().into(),
    };
    let description = option_str_tokens(doc_info.description.as_deref());
    let alias_descriptors = module_options.aliases.iter().map(|ty| {
        quote! {
            <#ty as ::harmony_luau::DescribeTypeAlias>::type_alias_descriptor()
        }
    });
    let interface_descriptors = module_options.interfaces.iter().map(|ty| {
        quote! {
            <#ty as ::harmony_luau::DescribeInterface>::interface_descriptor()
        }
    });
    let class_descriptors = module_options.classes.iter().map(|ty| {
        quote! {
            <#ty as ::harmony_luau::DescribeUserData>::class_descriptor()
        }
    });

    let mut function_descriptors = Vec::new();
    let mut function_registrations = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };

        let options = match parse_harmony_fn_options(&fn_item.attrs) {
            Ok(options) => options,
            Err(error) => return error.into_compile_error().into(),
        };
        strip_helper_attributes(&mut fn_item.attrs, "harmony");
        let doc_info = match extract_doc_info(&fn_item.attrs) {
            Ok(info) => info,
            Err(error) => return error.into_compile_error().into(),
        };
        if options.skip {
            continue;
        }

        if fn_item.sig.receiver().is_some() {
            return syn::Error::new(
                fn_item.sig.span(),
                "module methods must be static functions without a self receiver",
            )
            .into_compile_error()
            .into();
        }

        let fn_name = fn_item.sig.ident.to_string();
        let path = options.path.clone().unwrap_or(fn_name);
        let path_segments: Vec<_> = path
            .split('.')
            .filter(|segment| !segment.is_empty())
            .map(str::to_string)
            .collect();
        if path_segments.is_empty() {
            return syn::Error::new(
                fn_item.sig.ident.span(),
                "module method path must contain at least one segment",
            )
            .into_compile_error()
            .into();
        }
        let path_literals: Vec<_> = path
            .split('.')
            .filter(|segment| !segment.is_empty())
            .map(|segment| lit_str(segment, fn_item.sig.ident.span()))
            .collect();

        let params = collect_parameters(&fn_item.sig.inputs);
        let param_descriptors =
            match parameter_descriptors(&options, &fn_item.sig.inputs, fn_item.sig.ident.span()) {
                Ok(descriptors) => descriptors,
                Err(error) => return error.into_compile_error().into(),
            };
        let return_tokens = match return_tokens_with_doc_info(&options, &fn_item.sig.output) {
            Ok(tokens) => tokens,
            Err(error) => return error.into_compile_error().into(),
        };
        let param_names: Vec<_> = params.iter().map(|(name, _, _)| *name).collect();
        let param_types: Vec<_> = params.iter().map(|(_, ty, _)| *ty).collect();
        let returns_result = returns_result_type(&fn_item.sig);
        let yields_tokens = if fn_item.sig.asyncness.is_some() {
            quote! { true }
        } else {
            quote! { false }
        };
        let function_description = option_str_tokens(doc_info.description.as_deref());
        let function_name = &fn_item.sig.ident;
        let registration = module_registration_tokens(
            function_name,
            &path_segments,
            fn_item.sig.asyncness.is_some(),
            first_argument_is_lua_context(&fn_item.sig.inputs),
            returns_result,
            &param_names,
            &param_types,
            module_options.plugin_scoped,
        );

        function_descriptors.push(quote! {
            ::harmony_luau::ModuleFunctionDescriptor {
                path: vec![#(#path_literals),*],
                description: #function_description,
                params: vec![#(#param_descriptors),*],
                returns: #return_tokens,
                yields: #yields_tokens,
            }
        });
        function_registrations.push(registration);
    }

    let runtime_impl = if let Some(module_path) = module_path {
        let module_path_lit = lit_str(&module_path, Span::call_site());
        quote! {
            #[automatically_derived]
            impl #self_ty {
                pub fn _harmony_module_table(lua: &::mlua::Lua) -> ::mlua::Result<::mlua::Table> {
                    let table = lua.create_table()?;
                    #(#function_registrations)*
                    Ok(table)
                }

                pub fn module() -> ::harmony_core::Module {
                    // The `scope` field is a placeholder — the caller's
                    // `plugin_surface_exports!` invocation overrides it
                    // with the real scope (id, description, danger). An
                    // empty `id` cannot appear in any manifest's scopes
                    // list, so forgetting to override fails load loudly.
                    ::harmony_core::Module {
                        path: #module_path_lit.into(),
                        setup: ::std::sync::Arc::new(|lua: &::mlua::Lua| {
                            Ok(Self::_harmony_module_table(lua)?)
                        }),
                        scope: ::harmony_core::Scope {
                            id: ::std::sync::Arc::from(""),
                            description: "",
                            danger: ::harmony_core::Danger::Negligible,
                        },
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #ast

        #[automatically_derived]
        impl ::harmony_luau::DescribeModule for #self_ty {
            fn module_descriptor() -> ::harmony_luau::ModuleDescriptor {
                let mut descriptor = ::harmony_luau::ModuleDescriptor::new(
                    #module_name_lit,
                    #local_name_lit,
                    #description,
                );
                descriptor.functions.extend(vec![#(#function_descriptors),*]);
                descriptor
            }
        }

        #[automatically_derived]
        impl #self_ty {
            pub fn _harmony_type_aliases() -> ::std::vec::Vec<::harmony_luau::TypeAliasDescriptor> {
                vec![#(#alias_descriptors),*]
            }

            pub fn _harmony_interfaces() -> ::std::vec::Vec<::harmony_luau::InterfaceDescriptor> {
                vec![#(#interface_descriptors),*]
            }

            pub fn _harmony_classes() -> ::std::vec::Vec<::harmony_luau::ClassDescriptor> {
                vec![#(#class_descriptors),*]
            }

            pub fn render_luau_definition() -> ::std::result::Result<String, ::std::fmt::Error> {
                ::harmony_luau::render_definition_file_with_support(
                    &<Self as ::harmony_luau::DescribeModule>::module_descriptor(),
                    &Self::_harmony_type_aliases(),
                    &Self::_harmony_interfaces(),
                    &Self::_harmony_classes(),
                )
            }
        }

        #runtime_impl
    }
    .into()
}

#[proc_macro_attribute]
pub fn globals(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut ast: syn::ItemImpl = parse_macro_input!(item as syn::ItemImpl);
    let self_ty = ast.self_ty.clone();

    let mut descriptors = Vec::new();
    let mut registrations = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };

        if fn_item.sig.asyncness.is_some() {
            return syn::Error::new(
                fn_item.sig.span(),
                "harmony globals must be synchronous; async would lose Lua stack attribution",
            )
            .into_compile_error()
            .into();
        }
        if fn_item.sig.receiver().is_some() {
            return syn::Error::new(
                fn_item.sig.span(),
                "harmony globals must be free-standing static methods, not instance methods",
            )
            .into_compile_error()
            .into();
        }

        let doc_info = match extract_doc_info(&fn_item.attrs) {
            Ok(info) => info,
            Err(error) => return error.into_compile_error().into(),
        };
        strip_helper_attributes(&mut fn_item.attrs, "harmony");

        let fn_name = fn_item.sig.ident.clone();
        let fn_name_str = lit_str(&fn_name.to_string(), fn_name.span());
        let description = option_str_tokens(doc_info.description.as_deref());

        descriptors.push(quote! {
            ::harmony_luau::GlobalFunctionDescriptor {
                name: #fn_name_str,
                description: #description,
                params: vec![::harmony_luau::ParameterDescriptor {
                    name: "values",
                    ty: ::harmony_luau::LuauType::any(),
                    description: None,
                    variadic: true,
                }],
                returns: vec![],
                yields: false,
            }
        });

        registrations.push(quote! {
            lua.globals().set(
                #fn_name_str,
                lua.create_function(|lua, args: ::mlua::MultiValue| {
                    <#self_ty>::#fn_name(lua, args)
                })?,
            )?;
        });
    }

    let helper_tokens = quote! {
        #[automatically_derived]
        impl #self_ty {
            #[doc(hidden)]
            pub fn _harmony_luau_globals()
                -> ::std::vec::Vec<::harmony_luau::GlobalFunctionDescriptor>
            {
                vec![#(#descriptors),*]
            }

            #[doc(hidden)]
            pub fn _harmony_install_globals(lua: &::mlua::Lua) -> ::mlua::Result<()> {
                #(#registrations)*
                Ok(())
            }

            pub fn render_luau_definition()
                -> ::std::result::Result<::std::string::String, ::std::fmt::Error>
            {
                ::harmony_luau::render_globals_definition(&Self::_harmony_luau_globals())
            }
        }
    };

    quote! {
        #ast
        #helper_tokens
    }
    .into()
}

#[proc_macro]
pub fn compile(input: TokenStream) -> TokenStream {
    let compile::CompileArgs {
        type_path,
        fields,
        methods,
        variants,
    } = match parse_compile_args(input) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let type_name_lit = lit_str(&type_name_from_type_path(&type_path), Span::call_site());

    let fields_enabled = fields.unwrap_or(false);
    let methods_enabled = methods.unwrap_or(false);
    let variants_enabled = variants.unwrap_or(false);

    let fields_call = if fields_enabled {
        quote! {
            Self::_harmony_userdata_fields(fields);
        }
    } else {
        quote! {}
    };

    let methods_call = if methods_enabled {
        quote! {
            Self::_harmony_userdata_methods(methods);
        }
    } else {
        quote! {}
    };

    let variant_eq_call = if variants_enabled {
        quote! {
            methods.add_meta_function(
                ::mlua::MetaMethod::Eq,
                |_, (lhs, rhs): (::mlua::AnyUserData, ::mlua::AnyUserData)| {
                    let lhs = lhs.borrow::<Self>();
                    let rhs = rhs.borrow::<Self>();
                    match (lhs, rhs) {
                        (Ok(lhs), Ok(rhs)) => Ok(*lhs == *rhs),
                        _ => Ok(false),
                    }
                },
            );
        }
    } else {
        quote! {}
    };

    let variant_fields_call = if variants_enabled {
        quote! {
            Self::_harmony_userdata_variant_fields(fields);
        }
    } else {
        quote! {}
    };

    let description_expr = if fields_enabled {
        quote! { Self::_harmony_luau_description() }
    } else {
        quote! { None }
    };

    let field_descriptors = match (fields_enabled, variants_enabled) {
        (true, true) => quote! {
            descriptor.fields.extend(Self::_harmony_luau_fields());
            descriptor.fields.extend(Self::_harmony_luau_variant_fields());
        },
        (true, false) => quote! {
            descriptor.fields.extend(Self::_harmony_luau_fields());
        },
        (false, true) => quote! {
            descriptor.fields.extend(Self::_harmony_luau_variant_fields());
        },
        (false, false) => quote! {},
    };

    let method_descriptors = if methods_enabled {
        quote! {
            descriptor.methods.extend(Self::_harmony_luau_methods());
        }
    } else {
        quote! {}
    };

    quote! {
        #[automatically_derived]
        impl mlua::UserData for #type_path {
            fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
                #fields_call
                #variant_fields_call
            }

            fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
                #methods_call
                #variant_eq_call
            }
        }

        #[automatically_derived]
        impl mlua::FromLua for #type_path {
            fn from_lua(value: mlua::Value, _lua: &mlua::Lua) -> mlua::Result<Self> {
                match value {
                    mlua::Value::UserData(user_data) => match user_data.borrow::<Self>() {
                        Ok(value) => Ok((*value).clone()),
                        Err(_) => Err(mlua::Error::FromLuaConversionError {
                            from: "UserData",
                            to: stringify!(#type_path).to_string(),
                            message: Some("userdata is not this exact Rust type".into()),
                        }),
                    },
                    _ => Err(mlua::Error::FromLuaConversionError {
                        from: value.type_name(),
                        to: stringify!(#type_path).to_string(),
                        message: Some("expected userdata created by harmony_macros".into()),
                    }),
                }
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::LuauTypeInfo for #type_path {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(#type_name_lit)
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeUserData for #type_path {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                let mut descriptor = ::harmony_luau::ClassDescriptor::new(
                    #type_name_lit,
                    #description_expr,
                );
                #field_descriptors
                #method_descriptors
                descriptor
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn load(input: TokenStream) -> TokenStream {
    let load::LoadInput {
        lua_expr,
        type_paths,
    } = parse_macro_input!(input as load::LoadInput);

    quote! {{
        let lua: &mlua::Lua = &#lua_expr;
        let globals: mlua::Table = lua.globals();

        #(
            globals.set(stringify!(#type_paths), lua.create_proxy::<#type_paths>()?)?;
        )*
    }}
    .into()
}

fn parse_module_options(attr: TokenStream) -> syn::Result<ModuleOptions> {
    if attr.is_empty() {
        return Ok(ModuleOptions::default());
    }

    let items = darling::ast::NestedMeta::parse_meta_list(attr.into())?;
    let mut scalar_items = Vec::new();
    let mut options = ModuleOptions::default();

    for item in items {
        match item {
            darling::ast::NestedMeta::Meta(meta) if meta.path().is_ident("aliases") => {
                extend_module_type_paths(&mut options.aliases, &meta)?;
            }
            darling::ast::NestedMeta::Meta(meta) if meta.path().is_ident("interfaces") => {
                extend_module_type_paths(&mut options.interfaces, &meta)?;
            }
            darling::ast::NestedMeta::Meta(meta) if meta.path().is_ident("classes") => {
                extend_module_type_paths(&mut options.classes, &meta)?;
            }
            other => scalar_items.push(other),
        }
    }

    let scalar = ModuleScalarOptions::from_list(&scalar_items)
        .map_err(|error| syn::Error::new(Span::call_site(), error.to_string()))?;
    options.name = scalar.name;
    options.local = scalar.local;
    options.path = scalar.path;
    options.plugin_scoped = scalar.plugin_scoped;
    Ok(options)
}

fn extend_module_type_paths(target: &mut Vec<TypePath>, meta: &Meta) -> syn::Result<()> {
    let Meta::List(list) = meta else {
        return Err(syn::Error::new(
            meta.span(),
            "module support types must be declared as a list",
        ));
    };

    let parsed = syn::parse2::<TypePathList>(list.tokens.clone())?;
    target.extend(parsed.0);
    Ok(())
}

fn parse_harmony_fn_options(attrs: &[Attribute]) -> syn::Result<HarmonyFnOptions> {
    let mut options = HarmonyFnOptions::default();

    for attr in attrs {
        if !attr.path().is_ident("harmony") {
            continue;
        }

        let Meta::List(list) = &attr.meta else {
            return Err(syn::Error::new(
                attr.span(),
                "harmony helper attributes must use #[harmony(...)]",
            ));
        };
        let parsed = syn::parse2::<HarmonyFnOptionList>(list.tokens.clone())?;

        for option in parsed.0 {
            match option {
                HarmonyFnOption::Path(path) => options.path = Some(path.value()),
                HarmonyFnOption::Skip => options.skip = true,
                HarmonyFnOption::Args(args) => {
                    options.args = Some(
                        args.0
                            .into_iter()
                            .map(|arg| DocParameterOverride {
                                name: arg.name.to_string(),
                                ty: arg.ty,
                            })
                            .collect(),
                    );
                }
                HarmonyFnOption::Returns(returns) => {
                    options.returns = Some(returns.0.into_iter().collect());
                }
            }
        }
    }

    Ok(options)
}

fn strip_helper_attributes(attrs: &mut Vec<Attribute>, attr_name: &str) {
    attrs.retain(|attr| !attr.path().is_ident(attr_name));
}

fn parameter_descriptors(
    options: &HarmonyFnOptions,
    inputs: &Punctuated<FnArg, Token![,]>,
    span: Span,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    if let Some(args) = &options.args {
        return Ok(args
            .iter()
            .map(|arg| {
                let name = lit_str(&arg.name, span);
                let ty = &arg.ty;
                quote! {
                    ::harmony_luau::ParameterDescriptor {
                        name: #name,
                        ty: <#ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                        description: None,
                        variadic: false,
                    }
                }
            })
            .collect());
    }

    Ok(collect_parameters(inputs)
        .iter()
        .map(|(_, ty, name_lit)| {
            quote! {
                ::harmony_luau::ParameterDescriptor {
                    name: #name_lit,
                    ty: <#ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                    description: None,
                    variadic: false,
                }
            }
        })
        .collect())
}

fn return_tokens_with_doc_info(
    options: &HarmonyFnOptions,
    output: &ReturnType,
) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(returns) = &options.returns {
        let luau_types = returns.iter().map(|ty| {
            quote! {
                <#ty as ::harmony_luau::LuauTypeInfo>::luau_type()
            }
        });
        return Ok(quote! {
            vec![#(#luau_types),*]
        });
    }

    Ok(return_tokens(output))
}

fn module_registration_tokens(
    fn_name: &Ident,
    path_segments: &[String],
    is_async: bool,
    has_lua_context: bool,
    returns_result: bool,
    param_names: &[&Ident],
    param_types: &[&Type],
    plugin_scoped: bool,
) -> proc_macro2::TokenStream {
    let function_tokens = if has_lua_context {
        if is_async {
            if plugin_scoped {
                let invocation = if returns_result {
                    quote! { Self::#fn_name(lua, __harmony_plugin_id, #(#param_names,)*).await }
                } else {
                    quote! { Ok(Self::#fn_name(lua, __harmony_plugin_id, #(#param_names,)*).await) }
                };
                quote! {
                    lua.create_async_function_with_prelude(
                        |lua| ::harmony_core::resolve_caller(lua),
                        |lua, __harmony_plugin_id, (#(#param_names,)*): (#(#param_types,)*)| async move {
                            #invocation
                        },
                    )?
                }
            } else {
                quote! { lua.create_async_function(Self::#fn_name)? }
            }
        } else {
            quote! { lua.create_function(Self::#fn_name)? }
        }
    } else {
        let invocation = if is_async {
            if plugin_scoped {
                if returns_result {
                    quote! { Self::#fn_name(__harmony_plugin_id, #(#param_names,)*).await }
                } else {
                    quote! { Ok(Self::#fn_name(__harmony_plugin_id, #(#param_names,)*).await) }
                }
            } else if returns_result {
                quote! { Self::#fn_name(#(#param_names,)*).await }
            } else {
                quote! { Ok(Self::#fn_name(#(#param_names,)*).await) }
            }
        } else if returns_result {
            quote! { Self::#fn_name(#(#param_names,)*) }
        } else {
            quote! { Ok(Self::#fn_name(#(#param_names,)*)) }
        };

        if is_async {
            if plugin_scoped {
                quote! {
                    lua.create_async_function_with_prelude(
                        |lua| ::harmony_core::resolve_caller(lua),
                        |_, __harmony_plugin_id, (#(#param_names,)*): (#(#param_types,)*)| async move {
                            #invocation
                        },
                    )?
                }
            } else {
                quote! {
                    lua.create_async_function(|_, (#(#param_names,)*): (#(#param_types,)*)| async move {
                        #invocation
                    })?
                }
            }
        } else {
            quote! {
                lua.create_function(|_, (#(#param_names,)*): (#(#param_types,)*)| {
                    #invocation
                })?
            }
        }
    };

    let leaf = lit_str(
        path_segments.last().expect("module path has a leaf"),
        fn_name.span(),
    );
    let parent_steps = path_segments[..path_segments.len().saturating_sub(1)]
        .iter()
        .enumerate()
        .map(|(index, segment)| {
            let source_ident = if index == 0 {
                format_ident!("__harmony_module_root")
            } else {
                format_ident!("__harmony_module_path_{index}")
            };
            let target_ident = format_ident!("__harmony_module_path_{}", index + 1);
            let segment_lit = lit_str(segment, fn_name.span());
            quote! {
                let #target_ident: ::mlua::Table = match #source_ident.get(#segment_lit) {
                    Ok(table) => table,
                    Err(_) => {
                        let nested = lua.create_table()?;
                        #source_ident.set(#segment_lit, nested.clone())?;
                        nested
                    }
                };
            }
        })
        .collect::<Vec<_>>();

    let target_ident = if path_segments.len() > 1 {
        format_ident!("__harmony_module_path_{}", path_segments.len() - 1)
    } else {
        format_ident!("__harmony_module_root")
    };

    quote! {
        let __harmony_module_root = table.clone();
        #(#parent_steps)*
        #target_ident.set(#leaf, #function_tokens)?;
    }
}

fn collect_parameters(inputs: &Punctuated<FnArg, Token![,]>) -> Vec<(&Ident, &Type, LitStr)> {
    inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(pat_type) => {
                if is_lua_context_type(pat_type.ty.as_ref())
                    || is_plugin_id_context_type(pat_type.ty.as_ref())
                {
                    return None;
                }

                match &*pat_type.pat {
                    Pat::Ident(pattern) => Some((
                        &pattern.ident,
                        pat_type.ty.as_ref(),
                        lit_str(&pattern.ident.to_string(), pattern.ident.span()),
                    )),
                    _ => None,
                }
            }
            FnArg::Receiver(_) => None,
        })
        .collect()
}

fn is_plugin_id_context_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    let Some(last) = type_path.path.segments.last() else {
        return false;
    };
    if last.ident != "Option" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return false;
    };
    let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
        return false;
    };
    let Type::Path(inner_path) = inner else {
        return false;
    };
    let Some(inner_last) = inner_path.path.segments.last() else {
        return false;
    };
    if inner_last.ident == "PluginId" {
        return true;
    }
    if inner_last.ident != "Arc" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(arc_args) = &inner_last.arguments else {
        return false;
    };
    let Some(syn::GenericArgument::Type(arc_inner)) = arc_args.args.first() else {
        return false;
    };
    let Type::Path(arc_inner_path) = arc_inner else {
        return false;
    };
    arc_inner_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident == "str")
        .unwrap_or(false)
}

fn first_argument_is_lua_context(inputs: &Punctuated<FnArg, Token![,]>) -> bool {
    inputs
        .iter()
        .find_map(|argument| match argument {
            FnArg::Typed(pat_type) => Some(is_lua_context_type(pat_type.ty.as_ref())),
            FnArg::Receiver(_) => None,
        })
        .unwrap_or(false)
}

fn is_lua_context_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => is_lua_type_path(type_path),
        Type::Reference(TypeReference { elem, .. }) => matches!(
            elem.as_ref(),
            Type::Path(type_path) if is_lua_type_path(type_path)
        ),
        _ => false,
    }
}

fn is_lua_type_path(type_path: &TypePath) -> bool {
    type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident == "Lua")
        .unwrap_or(false)
}

fn return_tokens(output: &ReturnType) -> proc_macro2::TokenStream {
    match effective_return_type(output) {
        Some(return_ty) => {
            quote! {
                <#return_ty as ::harmony_luau::LuauReturn>::luau_returns()
            }
        }
        None => {
            quote! {
                ::std::vec::Vec::new()
            }
        }
    }
}

fn effective_return_type(output: &ReturnType) -> Option<Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };

    extract_result_inner_type(ty).or_else(|| Some((**ty).clone()))
}

fn extract_result_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }

    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };

    for argument in &arguments.args {
        if let syn::GenericArgument::Type(argument_ty) = argument {
            return Some(argument_ty.clone());
        }
    }

    None
}

fn returns_result_type(signature: &syn::Signature) -> bool {
    effective_return_type(&signature.output)
        .and_then(|_| match &signature.output {
            ReturnType::Type(_, ty) => extract_result_inner_type(ty),
            ReturnType::Default => None,
        })
        .is_some()
}

fn extract_doc_info(attrs: &[Attribute]) -> syn::Result<DocInfo> {
    Ok(DocInfo {
        description: trim_doc_lines(raw_doc_lines(attrs)).map(|lines| lines.join("\n")),
    })
}

fn raw_doc_lines(attrs: &[Attribute]) -> Vec<String> {
    let mut lines = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }

        let Meta::NameValue(meta) = &attr.meta else {
            continue;
        };
        let Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) = &meta.value
        else {
            continue;
        };

        let mut line = value.value();
        if line.starts_with(' ') {
            line.remove(0);
        }
        lines.push(line);
    }

    lines
}

fn trim_doc_lines(lines: Vec<String>) -> Option<Vec<String>> {
    let mut start = 0usize;
    let mut end = lines.len();

    while start < end && lines[start].trim().is_empty() {
        start += 1;
    }

    while end > start && lines[end - 1].trim().is_empty() {
        end -= 1;
    }

    if start == end {
        return None;
    }

    Some(lines[start..end].to_vec())
}

fn type_name_from_type(ty: &Type) -> String {
    match ty {
        Type::Path(path) => type_name_from_type_path(path),
        _ => quote!(#ty).to_string().replace(' ', ""),
    }
}

fn type_name_from_type_path(path: &TypePath) -> String {
    path.path
        .segments
        .last()
        .expect("type path has at least one segment")
        .ident
        .to_string()
}

fn default_module_local_name(type_name: &str) -> String {
    let mut output = String::new();
    for (index, ch) in type_name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 && !output.ends_with('_') {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch);
        }
    }

    output
        .strip_suffix("_module")
        .unwrap_or(output.as_str())
        .to_string()
}

fn option_str_tokens(value: Option<&str>) -> proc_macro2::TokenStream {
    match value {
        Some(value) => {
            let value = lit_str(value, Span::call_site());
            quote! { Some(#value) }
        }
        None => quote! { None },
    }
}

fn lit_str(value: &str, span: Span) -> LitStr {
    LitStr::new(value.strip_prefix("r#").unwrap_or(value), span)
}
