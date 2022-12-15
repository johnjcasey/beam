// Mocks generated by Mockito 5.2.0 from annotations
// in playground/test/pages/playground/states/example_selector_state_test.dart.
// Do not manually edit this file.

import 'dart:async' as _i5;

import 'package:mockito/mockito.dart' as _i1;
import 'package:playground_components/src/controllers/example_loaders/example_loader_factory.dart'
    as _i2;
import 'package:playground_components/src/controllers/example_loaders/examples_loader.dart'
    as _i3;
import 'package:playground_components/src/controllers/playground_controller.dart'
    as _i4;
import 'package:playground_components/src/models/example_loading_descriptors/examples_loading_descriptor.dart'
    as _i6;
import 'package:playground_components/src/models/sdk.dart' as _i7;

// ignore_for_file: type=lint
// ignore_for_file: avoid_redundant_argument_values
// ignore_for_file: avoid_setters_without_getters
// ignore_for_file: comment_references
// ignore_for_file: implementation_imports
// ignore_for_file: invalid_use_of_visible_for_testing_member
// ignore_for_file: prefer_const_constructors
// ignore_for_file: unnecessary_parenthesis
// ignore_for_file: camel_case_types

class _FakeExampleLoaderFactory_0 extends _i1.Fake
    implements _i2.ExampleLoaderFactory {}

/// A class which mocks [ExamplesLoader].
///
/// See the documentation for Mockito's code generation for more information.
class MockExamplesLoader extends _i1.Mock implements _i3.ExamplesLoader {
  MockExamplesLoader() {
    _i1.throwOnMissingStub(this);
  }

  @override
  _i2.ExampleLoaderFactory get defaultFactory => (super.noSuchMethod(
      Invocation.getter(#defaultFactory),
      returnValue: _FakeExampleLoaderFactory_0()) as _i2.ExampleLoaderFactory);
  @override
  void setPlaygroundController(_i4.PlaygroundController? value) =>
      super.noSuchMethod(Invocation.method(#setPlaygroundController, [value]),
          returnValueForMissingStub: null);
  @override
  _i5.Future<void> load(_i6.ExamplesLoadingDescriptor? descriptor) =>
      (super.noSuchMethod(Invocation.method(#load, [descriptor]),
          returnValue: Future<void>.value(),
          returnValueForMissingStub: Future<void>.value()) as _i5.Future<void>);
  @override
  _i5.Future<void> loadDefaultIfAny(_i7.Sdk? sdk) =>
      (super.noSuchMethod(Invocation.method(#loadDefaultIfAny, [sdk]),
          returnValue: Future<void>.value(),
          returnValueForMissingStub: Future<void>.value()) as _i5.Future<void>);
}
