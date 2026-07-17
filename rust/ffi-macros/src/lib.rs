use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, ReturnType, Type, parse_macro_input};

/// Contains a panic at an exported C boundary and converts it into the FFI's
/// internal-error channel. Infallible exports return a safe zero value after
/// recording the panic so unwinding never crosses into Go.
#[proc_macro_attribute]
pub fn ffi_guard(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut function = parse_macro_input!(item as ItemFn);
    let original = function.block;

    let panic_result = match &function.sig.output {
        ReturnType::Type(_, ty) if is_i32(ty) => quote! {
            crate::error::set_panic_error(payload.as_ref())
        },
        ReturnType::Type(_, ty) if matches!(ty.as_ref(), Type::Ptr(ptr) if ptr.mutability.is_some()) =>
        {
            quote! {{
                crate::error::record_panic(payload.as_ref());
                ::std::ptr::null_mut()
            }}
        }
        ReturnType::Type(_, ty) if matches!(ty.as_ref(), Type::Ptr(ptr) if ptr.mutability.is_none()) =>
        {
            quote! {{
                crate::error::record_panic(payload.as_ref());
                ::std::ptr::null()
            }}
        }
        _ => quote! {{
            crate::error::record_panic(payload.as_ref());
            ::std::default::Default::default()
        }},
    };

    function.block = Box::new(syn::parse_quote!({
        match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| #original)) {
            Ok(value) => value,
            Err(payload) => #panic_result,
        }
    }));
    quote!(#function).into()
}

fn is_i32(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("i32"))
}
