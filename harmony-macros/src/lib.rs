// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{
    Span,
    TokenStream as TokenStream2,
};
use quote::quote;
use syn::{
    Attribute,
    Fields,
    FnArg,
    Ident,
    ImplItem,
    Item,
    ItemEnum,
    ItemImpl,
    ItemStruct,
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

#[derive(Default)]
struct UserDataOptions {
    name: Option<LitStr>,
    description: Option<LitStr>,
}

struct UserDataOptionList(Punctuated<UserDataOption, Token![,]>);

enum UserDataOption {
    Name(LitStr),
    Description(LitStr),
}

struct HarmonyMethodOptions {
    skip: bool,
    name: Option<LitStr>,
    args: Option<Vec<DocParameterOverride>>,
    returns: Option<Vec<Type>>,
    description: Option<LitStr>,
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

enum HarmonyMethodOption {
    Skip,
    Name(LitStr),
    Args(DocParameterList),
    Returns(DocReturnList),
    Description(LitStr),
}

struct HarmonyMethodOptionList(Punctuated<HarmonyMethodOption, Token![,]>);

struct MethodParameter<'a> {
    ident: &'a Ident,
    ty: &'a Type,
    name_lit: LitStr,
    /// A trailing `Variadic<T>` that takes the remaining arguments.
    variadic: bool,
}

impl Parse for UserDataOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for UserDataOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        let _: Token![=] = input.parse()?;
        let value: LitStr = input.parse()?;

        if ident == "name" {
            return Ok(Self::Name(value));
        }
        if ident == "description" {
            return Ok(Self::Description(value));
        }

        Err(syn::Error::new(
            ident.span(),
            "unsupported userdata option; expected name or description",
        ))
    }
}

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

impl Parse for HarmonyMethodOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;

        if ident == "skip" {
            return Ok(Self::Skip);
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

        if ident == "description" {
            let _: Token![=] = input.parse()?;
            return Ok(Self::Description(input.parse()?));
        }

        if ident == "name" {
            let _: Token![=] = input.parse()?;
            return Ok(Self::Name(input.parse()?));
        }

        Err(syn::Error::new(
            ident.span(),
            "unsupported harmony option; expected skip, name, args, returns, or description",
        ))
    }
}

impl Parse for HarmonyMethodOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

#[proc_macro_attribute]
pub fn userdata(attr: TokenStream, item: TokenStream) -> TokenStream {
    let options = match parse_userdata_options(attr) {
        Ok(options) => options,
        Err(error) => return error.into_compile_error().into(),
    };
    let ast = parse_macro_input!(item as Item);

    match ast {
        Item::Struct(ast) => userdata_struct(ast, options).into(),
        Item::Enum(ast) => match userdata_enum(ast, options) {
            Ok(tokens) => tokens.into(),
            Err(error) => error.into_compile_error().into(),
        },
        item => syn::Error::new(
            item.span(),
            "userdata must be used on a struct or unit-variant enum",
        )
        .into_compile_error()
        .into(),
    }
}

fn userdata_struct(ast: ItemStruct, options: UserDataOptions) -> TokenStream2 {
    let type_name = &ast.ident;
    let type_name_lit = options
        .name
        .unwrap_or_else(|| lit_str(&type_name.to_string(), type_name.span()));
    let description_expr = option_lit_str_tokens(options.description.as_ref());
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    quote! {
        #ast

        #[automatically_derived]
        impl #impl_generics ::harmony_core::UserDataType for #type_name #ty_generics #where_clause {
            const CLASS_NAME: &'static str = #type_name_lit;
            const DESCRIPTION: ::std::option::Option<&'static str> = #description_expr;
        }

        #[automatically_derived]
        impl #impl_generics ::harmony_luau::LuauTypeInfo for #type_name #ty_generics #where_clause {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(<Self as ::harmony_core::UserDataType>::CLASS_NAME)
            }
        }
    }
}

fn userdata_enum(ast: ItemEnum, options: UserDataOptions) -> syn::Result<TokenStream2> {
    let type_name = &ast.ident;
    if !ast.generics.params.is_empty() || ast.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            ast.generics.span(),
            "userdata enums cannot be generic",
        ));
    }

    let mut variant_names = Vec::new();
    let mut variant_values = Vec::new();
    let mut field_descriptors = Vec::new();
    for variant in &ast.variants {
        if !matches!(&variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "userdata enum variants must be unit variants",
            ));
        }

        let variant_name = &variant.ident;
        let variant_name_lit = lit_str(&variant_name.to_string(), variant_name.span());
        variant_names.push(variant_name_lit.clone());
        variant_values.push(quote! { #type_name::#variant_name });
        field_descriptors.push(quote! {
            ::harmony_luau::FieldDescriptor {
                name: #variant_name_lit,
                ty: <Self as ::harmony_luau::LuauTypeInfo>::luau_type(),
                description: None,
            }
        });
    }

    let type_name_lit = options
        .name
        .unwrap_or_else(|| lit_str(&type_name.to_string(), type_name.span()));
    let description_expr = option_lit_str_tokens(options.description.as_ref());

    Ok(quote! {
        #ast

        #[automatically_derived]
        impl ::harmony_core::UserDataType for #type_name {
            const CLASS_NAME: &'static str = #type_name_lit;
            const DESCRIPTION: ::std::option::Option<&'static str> = #description_expr;
        }

        #[automatically_derived]
        impl ::harmony_luau::LuauTypeInfo for #type_name {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(<Self as ::harmony_core::UserDataType>::CLASS_NAME)
            }
        }

        #[automatically_derived]
        impl ::harmony_core::UserDataBinding for #type_name {
            fn class() -> &'static ::harmony_core::UserDataClass<Self> {
                static CLASS: ::std::sync::LazyLock<::harmony_core::UserDataClass<#type_name>> =
                    ::std::sync::LazyLock::new(|| {
                        ::harmony_core::UserDataClass::new(
                            <#type_name as ::harmony_core::UserDataType>::CLASS_NAME,
                        )
                            .equality()
                            #(.variant(#variant_names, #variant_values))*
                    });
                &CLASS
            }

            fn spec() -> ::harmony_core::UserDataSpec {
                ::harmony_core::UserDataSpec::new(
                    <#type_name as ::harmony_core::UserDataType>::CLASS_NAME,
                )
                    .initializer(|vm, origin, root| {
                        <#type_name as ::harmony_core::UserDataBinding>::class()
                            .install_variant_table(vm, origin, root)
                    })
            }
        }

        #[automatically_derived]
        impl<'vm> ::harmony_luau::FromLuau<'vm> for #type_name {
            fn read(reader: &mut ::harmony_luau::ArgReader<'vm>) -> ::harmony_luau::runtime::Result<Self> {
                let userdata = <::harmony_luau::UserData as ::harmony_luau::FromLuau<'vm>>::read(reader)?;
                <Self as ::harmony_core::UserDataBinding>::class().read_userdata(reader.vm(), &userdata)
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeUserData for #type_name {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                let mut descriptor = ::harmony_luau::ClassDescriptor::new(
                    <Self as ::harmony_core::UserDataType>::CLASS_NAME,
                    <Self as ::harmony_core::UserDataType>::DESCRIPTION,
                );
                descriptor.fields.extend(vec![#(#field_descriptors),*]);
                descriptor
            }
        }
    })
}

#[proc_macro_attribute]
pub fn userdata_methods(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut ast = parse_macro_input!(item as ItemImpl);
    if ast.trait_.is_some() {
        return syn::Error::new(
            ast.impl_token.span,
            "userdata_methods must be used on an inherent impl",
        )
        .into_compile_error()
        .into();
    }

    let self_ty = ast.self_ty.clone();
    let mut class_methods = Vec::new();
    let mut method_specs = Vec::new();
    let mut method_descriptors = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };

        let options = match parse_harmony_method_options(&fn_item.attrs) {
            Ok(options) => options,
            Err(error) => return error.into_compile_error().into(),
        };
        strip_helper_attributes(&mut fn_item.attrs);

        if options.skip {
            continue;
        }

        let receiver = match fn_item.sig.receiver() {
            Some(receiver) => receiver,
            None => {
                return syn::Error::new(
                    fn_item.sig.ident.span(),
                    "userdata methods must take self",
                )
                .into_compile_error()
                .into();
            }
        };
        if receiver.mutability.is_some() {
            return syn::Error::new(
                receiver.span(),
                "mutable userdata receivers are not supported; use interior mutability",
            )
            .into_compile_error()
            .into();
        }

        let fn_name = &fn_item.sig.ident;
        let fn_name_lit = options
            .name
            .clone()
            .unwrap_or_else(|| lit_str(&fn_name.to_string(), fn_name.span()));
        let description = option_lit_str_tokens(options.description.as_ref());
        let kind = CallKind::of(&fn_item.sig);
        let parameters = match function_parameters(&fn_item.sig.inputs) {
            Ok(parameters) => parameters,
            Err(error) => return error.into_compile_error().into(),
        };
        if kind == CallKind::Async && has_frame_parameter(&parameters) {
            return syn::Error::new(
                fn_item.sig.ident.span(),
                "async userdata methods cannot take borrowed Luau context; convert Luau values before creating the future",
            )
            .into_compile_error()
            .into();
        }

        let read_parameters = read_parameter_tokens(&parameters);
        let call_args = call_argument_tokens(&parameters);
        let arguments = luau_arguments(&parameters);
        let method_argument_names = std::iter::once(lit_str("self", fn_name.span()))
            .chain(named_arguments(&arguments).map(|param| param.name_lit.clone()))
            .collect::<Vec<_>>();
        let spec_argument_names = named_arguments(&arguments).map(|param| &param.name_lit);
        method_specs.push(quote! {
            ::harmony_core::FunctionSpec::new(#fn_name_lit)
                #(.arg_name(#spec_argument_names))*
        });
        let call = quote! { __harmony_this.#fn_name(#(#call_args),*) };
        let body = call_body(kind, &call, &fn_item.sig.output);
        let registration = match kind {
            CallKind::Sync => quote! {
                .method(#fn_name_lit, [#(#method_argument_names),*], |__harmony_this: #self_ty, mut frame: ::harmony_luau::CallFrame<'_>| {
                    #read_parameters
                    #body
                })
            },
            CallKind::Async | CallKind::ScheduledFuture => quote! {
                .async_method(#fn_name_lit, [#(#method_argument_names),*], |__harmony_this: #self_ty, mut frame: ::harmony_luau::AsyncCallFrame<'_>| {
                    #read_parameters
                    #body
                })
            },
        };
        class_methods.push(registration);

        let params = parameter_descriptors(&options, &arguments, fn_item.sig.ident.span());
        let returns = return_descriptor_tokens(&options, &fn_item.sig.output);
        let yields = kind.yields();
        method_descriptors.push(quote! {
            ::harmony_luau::MethodDescriptor {
                name: #fn_name_lit,
                description: #description,
                params: vec![#(#params),*],
                returns: #returns,
                yields: #yields,
                kind: ::harmony_luau::MethodKind::Instance,
            }
        });
    }
    strip_context_attributes(&mut ast);

    quote! {
        #ast

        #[automatically_derived]
        impl ::harmony_core::UserDataBinding for #self_ty {
            fn class() -> &'static ::harmony_core::UserDataClass<#self_ty> {
                static CLASS: ::std::sync::LazyLock<::harmony_core::UserDataClass<#self_ty>> =
                    ::std::sync::LazyLock::new(|| {
                        let class: ::harmony_core::UserDataClass<#self_ty> =
                            ::harmony_core::UserDataClass::new(
                            <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                        );
                        class
                            #(#class_methods)*
                    });
                &CLASS
            }

            fn spec() -> ::harmony_core::UserDataSpec {
                ::harmony_core::UserDataSpec::new(
                    <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                )
                    #(.method(#method_specs))*
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeUserData for #self_ty {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                let mut descriptor = ::harmony_luau::ClassDescriptor::new(
                    <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                    <#self_ty as ::harmony_core::UserDataType>::DESCRIPTION,
                );
                descriptor.methods.extend(vec![#(#method_descriptors),*]);
                descriptor
            }
        }
    }
    .into()
}

#[derive(Default)]
struct ModuleOptions {
    id: Option<LitStr>,
    name: Option<LitStr>,
    local: Option<LitStr>,
    description: Option<LitStr>,
    docs_feature: Option<LitStr>,
    /// Userdata types whose classes the module registers.
    userdata: Vec<Type>,
    /// A `fn(ModuleSpec) -> ModuleSpec` adding what the functions don't, such as initializers.
    extend: Option<syn::Path>,
    /// A `fn(&mut ModuleDescriptor)` adding what the functions don't, such as fields.
    describe: Option<syn::Path>,
}

struct ModuleOptionList(Punctuated<ModuleOption, Token![,]>);

enum ModuleOption {
    Text(Ident, LitStr),
    Path(Ident, syn::Path),
    Userdata(Punctuated<Type, Token![,]>),
}

impl Parse for ModuleOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for ModuleOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if key == "userdata" {
            let content;
            syn::parenthesized!(content in input);
            return Ok(Self::Userdata(Punctuated::parse_terminated(&content)?));
        }
        let _: Token![=] = input.parse()?;
        if key == "extend" || key == "describe" {
            return Ok(Self::Path(key, input.parse()?));
        }
        Ok(Self::Text(key, input.parse()?))
    }
}

fn parse_module_options(attr: TokenStream) -> syn::Result<ModuleOptions> {
    let mut options = ModuleOptions::default();
    for option in syn::parse::<ModuleOptionList>(attr)?.0 {
        match option {
            ModuleOption::Userdata(types) => options.userdata.extend(types),
            ModuleOption::Path(key, path) => {
                if key == "extend" {
                    options.extend = Some(path);
                } else {
                    options.describe = Some(path);
                }
            }
            ModuleOption::Text(key, value) => {
                let slot = match key.to_string().as_str() {
                    "id" => &mut options.id,
                    "name" => &mut options.name,
                    "local" => &mut options.local,
                    "description" => &mut options.description,
                    "docs_feature" => &mut options.docs_feature,
                    _ => {
                        return Err(syn::Error::new(
                            key.span(),
                            "unsupported module option; expected id, name, local, description, docs_feature, userdata, extend, or describe",
                        ));
                    }
                };
                *slot = Some(value);
            }
        }
    }
    Ok(options)
}

/// How a function's parameter is filled.
enum Parameter<'a> {
    /// A Luau argument, read by name.
    Argument(MethodParameter<'a>),
    /// A value taken from the call through `FromCallFrame`.
    Context { ident: &'a Ident, ty: &'a Type },
    /// The VM, call context, or chunk origin of a synchronous call, borrowed from its frame.
    Frame(TokenStream2),
}

/// Classifies every parameter but the receiver.
fn function_parameters(inputs: &Punctuated<FnArg, Token![,]>) -> syn::Result<Vec<Parameter<'_>>> {
    let parameters = inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(pat_type) => Some(pat_type),
            FnArg::Receiver(_) => None,
        })
        .map(|pat_type| {
            let Pat::Ident(pattern) = &*pat_type.pat else {
                return Err(syn::Error::new(
                    pat_type.pat.span(),
                    "parameters must be plain identifiers",
                ));
            };
            let ty = pat_type.ty.as_ref();
            if pat_type
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("context"))
            {
                return Ok(Parameter::Context {
                    ident: &pattern.ident,
                    ty,
                });
            }
            if let Some(frame) = frame_borrow(ty) {
                return Ok(Parameter::Frame(frame));
            }
            if is_frame_type(ty) {
                return Err(syn::Error::new(
                    ty.span(),
                    "borrow the VM, call context, or chunk origin, or take it by value with #[context]",
                ));
            }
            Ok(Parameter::Argument(MethodParameter {
                ident: &pattern.ident,
                ty,
                name_lit: argument_name(&pattern.ident),
                variadic: is_variadic_type(ty),
            }))
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let arguments = luau_arguments(&parameters);
    if let Some((_, leading)) = arguments.split_last()
        && let Some(argument) = leading.iter().find(|argument| argument.variadic)
    {
        return Err(syn::Error::new(
            argument.ty.span(),
            "a Variadic parameter must be the last Luau argument",
        ));
    }
    Ok(parameters)
}

/// The Luau name of an argument: its identifier without a leading `_`, so a parameter the
/// function ignores keeps its documented name.
fn argument_name(ident: &Ident) -> LitStr {
    let name = ident.to_string();
    let name = name.strip_prefix("r#").unwrap_or(&name);
    lit_str(name.strip_prefix('_').unwrap_or(name), ident.span())
}

fn frame_borrow(ty: &Type) -> Option<TokenStream2> {
    let Type::Reference(TypeReference { elem, .. }) = ty else {
        return None;
    };
    if is_type_named(elem, "Vm") {
        Some(quote! { frame.vm })
    } else if is_type_named(elem, "CallContext") {
        Some(quote! { &frame.context })
    } else if is_type_named(elem, "ChunkOrigin") {
        Some(quote! { &frame.context.origin })
    } else {
        None
    }
}

fn is_variadic_type(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if is_type_path_named(path, "Variadic"))
}

fn is_frame_type(ty: &Type) -> bool {
    is_type_named(ty, "Vm") || is_type_named(ty, "CallContext") || is_type_named(ty, "ChunkOrigin")
}

fn has_frame_parameter(parameters: &[Parameter<'_>]) -> bool {
    parameters
        .iter()
        .any(|parameter| matches!(parameter, Parameter::Frame(_)))
}

fn luau_arguments<'a>(parameters: &'a [Parameter<'a>]) -> Vec<&'a MethodParameter<'a>> {
    parameters
        .iter()
        .filter_map(|parameter| match parameter {
            Parameter::Argument(argument) => Some(argument),
            _ => None,
        })
        .collect()
}

/// The arguments Luau labels in errors by name; a trailing `Variadic` has none.
fn named_arguments<'a>(
    arguments: &'a [&'a MethodParameter<'a>],
) -> impl Iterator<Item = &'a MethodParameter<'a>> {
    arguments
        .iter()
        .copied()
        .filter(|argument| !argument.variadic)
}

/// Reads the Luau arguments by name, then the context values.
fn read_parameter_tokens(parameters: &[Parameter<'_>]) -> TokenStream2 {
    let arguments = luau_arguments(parameters).into_iter().map(|argument| {
        let ident = argument.ident;
        let ty = argument.ty;
        let name = &argument.name_lit;
        if argument.variadic {
            quote! { let #ident: #ty = frame.args.read()?; }
        } else {
            quote! { let #ident: #ty = frame.args.read_named(#name)?; }
        }
    });
    let context = parameters.iter().filter_map(|parameter| match parameter {
        Parameter::Context { ident, ty } => Some(quote! {
            let #ident = <#ty as ::harmony_luau::FromCallFrame>::from_call_frame(
                frame.vm,
                &frame.context,
            )?;
        }),
        _ => None,
    });
    quote! {
        #(#arguments)*
        #(#context)*
    }
}

fn call_argument_tokens(parameters: &[Parameter<'_>]) -> Vec<TokenStream2> {
    parameters
        .iter()
        .map(|parameter| match parameter {
            Parameter::Argument(argument) => {
                let ident = argument.ident;
                quote! { #ident }
            }
            Parameter::Context { ident, .. } => quote! { #ident },
            Parameter::Frame(tokens) => tokens.clone(),
        })
        .collect()
}

fn strip_context_attributes(ast: &mut ItemImpl) {
    for item in &mut ast.items {
        if let ImplItem::Fn(fn_item) = item {
            for input in &mut fn_item.sig.inputs {
                if let FnArg::Typed(pat_type) = input {
                    pat_type
                        .attrs
                        .retain(|attr| !attr.path().is_ident("context"));
                }
            }
        }
    }
}

/// How a function's result reaches Luau.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CallKind {
    /// Written to the frame's returns.
    Sync,
    /// An `async fn`, awaited in a scheduled future.
    Async,
    /// A synchronous function returning the `ScheduledFuture` to yield on.
    ScheduledFuture,
}

impl CallKind {
    fn of(signature: &syn::Signature) -> Self {
        if signature.asyncness.is_some() {
            Self::Async
        } else if returns_scheduled_future(&signature.output) {
            Self::ScheduledFuture
        } else {
            Self::Sync
        }
    }

    fn yields(self) -> bool {
        self != Self::Sync
    }
}

/// The body of a native function's closure once its parameters are read: `call` invoked and its
/// result returned to Luau.
fn call_body(kind: CallKind, call: &TokenStream2, output: &ReturnType) -> TokenStream2 {
    match kind {
        CallKind::Sync => sync_call_body(call, output),
        CallKind::Async => async_call_body(call, output),
        CallKind::ScheduledFuture => scheduled_future_call_body(call, output),
    }
}

/// Turns an inherent impl of associated functions into a Luau module: `module_spec()` registers
/// each function, reading its parameters as Luau arguments by name, and `DescribeModule`
/// documents them from their types. A `#[context]` parameter comes from the call through
/// `FromCallFrame` rather than from the arguments; an `async fn` yields.
#[proc_macro_attribute]
pub fn module(attr: TokenStream, item: TokenStream) -> TokenStream {
    let options = match parse_module_options(attr) {
        Ok(options) => options,
        Err(error) => return error.into_compile_error().into(),
    };
    let ast = parse_macro_input!(item as ItemImpl);
    match module_impl(ast, options) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn module_impl(mut ast: ItemImpl, options: ModuleOptions) -> syn::Result<TokenStream2> {
    if ast.trait_.is_some() {
        return Err(syn::Error::new(
            ast.impl_token.span,
            "module must be used on an inherent impl",
        ));
    }
    let Some(id) = options.id else {
        return Err(syn::Error::new(
            ast.impl_token.span,
            "module requires id = \"...\"",
        ));
    };
    let self_ty = ast.self_ty.clone();
    let type_name = match self_ty.as_ref() {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    };
    let name = options.name.unwrap_or_else(|| {
        lit_str(
            type_name.strip_suffix("Module").unwrap_or(&type_name),
            id.span(),
        )
    });
    let local = options.local.unwrap_or_else(|| {
        let id_value = id.value();
        lit_str(id_value.rsplit('/').next().unwrap_or(&id_value), id.span())
    });
    let description = option_lit_str_tokens(options.description.as_ref());

    let mut function_specs = Vec::new();
    let mut function_descriptors = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };
        let method_options = parse_harmony_method_options(&fn_item.attrs)?;
        strip_helper_attributes(&mut fn_item.attrs);
        if method_options.skip {
            continue;
        }

        let fn_name = &fn_item.sig.ident;
        if let Some(receiver) = fn_item.sig.receiver() {
            return Err(syn::Error::new(
                receiver.span(),
                "module functions are associated functions and cannot take self",
            ));
        }
        let lua_name = method_options
            .name
            .clone()
            .unwrap_or_else(|| lit_str(&fn_name.to_string(), fn_name.span()));
        let kind = CallKind::of(&fn_item.sig);
        let parameters = function_parameters(&fn_item.sig.inputs)?;

        if kind == CallKind::Async && has_frame_parameter(&parameters) {
            return Err(syn::Error::new(
                fn_name.span(),
                "async module functions cannot borrow the VM or call context; take #[context] values instead",
            ));
        }

        let arguments = luau_arguments(&parameters);
        let read_parameters = read_parameter_tokens(&parameters);
        let call_arguments = call_argument_tokens(&parameters);
        let call = quote! { <#self_ty>::#fn_name(#(#call_arguments),*) };
        let body = call_body(kind, &call, &fn_item.sig.output);
        let argument_names = named_arguments(&arguments).map(|argument| &argument.name_lit);

        let spec = match kind {
            CallKind::Sync => quote! {
                ::harmony_core::FunctionSpec::new(#lua_name)
                    #(.arg_name(#argument_names))*
                    .call(|mut frame: ::harmony_luau::CallFrame<'_>| {
                        #read_parameters
                        #body
                    })
            },
            CallKind::Async | CallKind::ScheduledFuture => quote! {
                ::harmony_core::FunctionSpec::new(#lua_name)
                    #(.arg_name(#argument_names))*
                    .call_async(::std::sync::Arc::new(|mut frame: ::harmony_luau::AsyncCallFrame<'_>| {
                        #read_parameters
                        #body
                    }))
            },
        };
        function_specs.push(spec);

        let path = lua_name
            .value()
            .split('.')
            .map(|segment| lit_str(segment, lua_name.span()))
            .collect::<Vec<_>>();
        let function_description = option_lit_str_tokens(method_options.description.as_ref());
        let params = parameter_descriptors(&method_options, &arguments, fn_name.span());
        let returns = return_descriptor_tokens(&method_options, &fn_item.sig.output);
        let yields = kind.yields();
        function_descriptors.push(quote! {
            ::harmony_luau::ModuleFunctionDescriptor {
                path: vec![#(#path),*],
                description: #function_description,
                params: vec![#(#params),*],
                returns: #returns,
                yields: #yields,
            }
        });
    }
    strip_context_attributes(&mut ast);

    let docs_cfg = options
        .docs_feature
        .map(|feature| quote! { #[cfg(feature = #feature)] });
    let userdata = &options.userdata;
    let extend = options
        .extend
        .as_ref()
        .map(|path| quote! { let spec = #path(spec); });
    let describe = options
        .describe
        .as_ref()
        .map(|path| quote! { #path(&mut descriptor); });

    Ok(quote! {
        #ast

        #[automatically_derived]
        impl #self_ty {
            /// The module's functions, as the host registers them.
            pub fn module_spec() -> ::harmony_core::ModuleSpec {
                let spec = ::harmony_core::ModuleSpec::new(#id)
                    #(.function(#function_specs))*
                    #(.userdata(<#userdata as ::harmony_core::UserDataBinding>::spec()))*;
                #extend
                spec
            }
        }

        #docs_cfg
        #[automatically_derived]
        impl ::harmony_luau::DescribeModule for #self_ty {
            fn module_descriptor() -> ::harmony_luau::ModuleDescriptor {
                #[allow(unused_mut)]
                let mut descriptor = ::harmony_luau::ModuleDescriptor {
                    name: #name,
                    local_name: #local,
                    description: #description,
                    fields: ::std::vec::Vec::new(),
                    functions: vec![#(#function_descriptors),*],
                };
                #describe
                descriptor
            }
        }
    })
}

fn parameter_descriptors(
    options: &HarmonyMethodOptions,
    arguments: &[&MethodParameter<'_>],
    span: Span,
) -> Vec<TokenStream2> {
    match &options.args {
        Some(overrides) => overrides
            .iter()
            .map(|argument| {
                parameter_descriptor_tokens(
                    &lit_str(&argument.name, span),
                    &argument.ty,
                    is_variadic_type(&argument.ty),
                )
            })
            .collect(),
        None => arguments
            .iter()
            .map(|argument| {
                parameter_descriptor_tokens(&argument.name_lit, argument.ty, argument.variadic)
            })
            .collect(),
    }
}

fn parameter_descriptor_tokens(name: &LitStr, ty: &Type, variadic: bool) -> TokenStream2 {
    quote! {
        ::harmony_luau::ParameterDescriptor {
            name: #name,
            ty: <#ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
            description: None,
            variadic: #variadic,
        }
    }
}

fn parse_userdata_options(attr: TokenStream) -> syn::Result<UserDataOptions> {
    if attr.is_empty() {
        return Ok(UserDataOptions::default());
    }

    let options = syn::parse::<UserDataOptionList>(attr)?;
    let mut parsed = UserDataOptions::default();
    for option in options.0 {
        match option {
            UserDataOption::Name(value) => parsed.name = Some(value),
            UserDataOption::Description(value) => parsed.description = Some(value),
        }
    }
    Ok(parsed)
}

fn parse_harmony_method_options(attrs: &[Attribute]) -> syn::Result<HarmonyMethodOptions> {
    let mut options = HarmonyMethodOptions {
        skip: false,
        name: None,
        args: None,
        returns: None,
        description: None,
    };

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
        let parsed = syn::parse2::<HarmonyMethodOptionList>(list.tokens.clone())?;

        for option in parsed.0 {
            match option {
                HarmonyMethodOption::Skip => options.skip = true,
                HarmonyMethodOption::Name(name) => options.name = Some(name),
                HarmonyMethodOption::Args(args) => {
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
                HarmonyMethodOption::Returns(returns) => {
                    options.returns = Some(returns.0.into_iter().collect());
                }
                HarmonyMethodOption::Description(description) => {
                    options.description = Some(description);
                }
            }
        }
    }

    Ok(options)
}

fn strip_helper_attributes(attrs: &mut Vec<Attribute>) {
    attrs.retain(|attr| !attr.path().is_ident("harmony"));
}

fn sync_call_body(call: &TokenStream2, output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! {
            #call;
            Ok(())
        },
        ReturnType::Type(_, ty) => {
            if let Some(inner) = extract_result_inner_type(ty) {
                if is_unit_type(&inner) {
                    quote! {
                        #call?;
                        Ok(())
                    }
                } else {
                    quote! {
                        let __harmony_result = #call?;
                        frame.returns.write(__harmony_result)
                    }
                }
            } else if is_unit_type(ty) {
                quote! {
                    #call;
                    Ok(())
                }
            } else {
                quote! {
                    let __harmony_result = #call;
                    frame.returns.write(__harmony_result)
                }
            }
        }
    }
}

fn async_call_body(call: &TokenStream2, output: &ReturnType) -> TokenStream2 {
    let call = quote! { #call.await };
    match output {
        ReturnType::Default => quote! {
            Ok(::harmony_luau::ScheduledFuture::new(async move {
                #call;
                Ok(())
            }))
        },
        ReturnType::Type(_, ty) => {
            if extract_result_inner_type(ty).is_some() {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        #call.map_err(::harmony_luau::Error::from)
                    }))
                }
            } else if is_unit_type(ty) {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        #call;
                        Ok(())
                    }))
                }
            } else {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        Ok(#call)
                    }))
                }
            }
        }
    }
}

fn scheduled_future_call_body(call: &TokenStream2, output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! {
            compile_error!("scheduled future functions must return ScheduledFuture");
        },
        ReturnType::Type(_, ty) => {
            if extract_result_inner_type(ty).is_some() {
                quote! {
                    #call.map_err(::harmony_luau::Error::from)
                }
            } else {
                quote! {
                    Ok(#call)
                }
            }
        }
    }
}

fn return_descriptor_tokens(options: &HarmonyMethodOptions, output: &ReturnType) -> TokenStream2 {
    if let Some(returns) = &options.returns {
        let luau_types = returns.iter().map(|ty| {
            quote! {
                <#ty as ::harmony_luau::LuauTypeInfo>::luau_type()
            }
        });
        return quote! {
            vec![#(#luau_types),*]
        };
    }

    if returns_scheduled_future(output) {
        return quote! {
            ::std::vec::Vec::new()
        };
    }

    match effective_return_type(output) {
        Some(return_ty) => quote! {
            <#return_ty as ::harmony_luau::LuauReturn>::luau_returns()
        },
        None => quote! {
            ::std::vec::Vec::new()
        },
    }
}

fn effective_return_type(output: &ReturnType) -> Option<Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    extract_result_inner_type(ty).or_else(|| Some((**ty).clone()))
}

fn returns_scheduled_future(output: &ReturnType) -> bool {
    effective_return_type(output)
        .as_ref()
        .is_some_and(is_scheduled_future_type)
}

fn is_scheduled_future_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "ScheduledFuture")
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

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn is_type_named(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Path(type_path) => is_type_path_named(type_path, name),
        Type::Reference(TypeReference { elem, .. }) => matches!(
            elem.as_ref(),
            Type::Path(type_path) if is_type_path_named(type_path, name)
        ),
        _ => false,
    }
}

fn is_type_path_named(type_path: &TypePath, name: &str) -> bool {
    type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident == name)
        .unwrap_or(false)
}

fn option_lit_str_tokens(value: Option<&LitStr>) -> TokenStream2 {
    match value {
        Some(value) => quote! { Some(#value) },
        None => quote! { None },
    }
}

fn lit_str(value: &str, span: Span) -> LitStr {
    LitStr::new(value.strip_prefix("r#").unwrap_or(value), span)
}
