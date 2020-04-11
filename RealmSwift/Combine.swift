////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#if canImport(Combine)
import Combine
import Realm.Private

// MARK: - Protocols

public protocol ObjectKeyIdentifable: Identifiable, Object {
    var id: UInt64 { get }
}

extension ObjectKeyIdentifable {
    public var id: UInt64 {
        RLMObjectBaseGetCombineId(self)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public protocol RealmSubscribable {
    func observe<S>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S: Subscriber, S.Input == Self
    func observe<S>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S: Subscriber, S.Input == (Self, ChangeSet)
    associatedtype ChangeSet
}

// MARK: - Object

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Object: Combine.ObservableObject {
    public var objectWillChange: RealmPublisher<Object> {
        return RealmPublisher(self)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Object: RealmSubscribable {
    public func observe<S: Subscriber>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S.Input: Object {
        return observe(on: queue) { change in
            switch change {
            case .change:
                _ = subscriber.receive((freeze ? self.freeze() : self) as! S.Input)
            case .deleted:
                subscriber.receive(completion: .finished)
            default:
                break
            }
        }
    }

    public func observe<S: Subscriber, T: Object>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S.Input == (T, ObjectChange) {
        return observe(on: queue) { change in
            _ = subscriber.receive(((freeze ? self.freeze() : self) as! T, change))
            // FIXME: finish, error, etc.
        }
    }

    public typealias ChangeSet = ObjectChange
}

// MARK: - List

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension List {
    /// Allows a subscriber to hook into Realm Changes.
    public func observe<S, V, L: List<V>>(_ subscriber: S) -> NotificationToken where S: Subscriber, S.Input == L {
        return observe { change in
            switch change {
            case .update(_, deletions: _, insertions: _, modifications: _):
                _ = subscriber.receive(self.freeze() as! L)
            default:
                break
            }
        }
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension List: ObservableObject {
    public var objectWillChange: RealmPublisher<List> {
        RealmPublisher(self)
    }
}

// MARK: - LinkingObjects

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
//extension LinkingObjects: Combine.ObservableObject, Identifiable {
extension LinkingObjects {
    public var objectWillChange: RealmPublisher<LinkingObjects> {
        RealmPublisher(self)
    }
}

// MARK: RealmCollection

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension RealmCollection {
    public func observe<S>(on queue: DispatchQueue? = nil, _ subscriber: S, freeze: Bool) -> NotificationToken where S: Subscriber, S.Input == Self {
        // FIXME: we could skip some pointless work in converting the changeset to the Swift type here
        return observe(on: queue) { change in
            switch change {
            case .update(_, deletions: _, insertions: _, modifications: _):
                _ = subscriber.receive(freeze ? self.freeze() : self)
            // FIXME: error
            default:
                break
            }
        }
    }

    public func observe<S>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S: Subscriber, S.Input == (Self, ChangeSet) {
        return observe(on: queue) { change in
            _ = subscriber.receive((freeze ? self.freeze() : self, change))
            // FIXME: finish on error? maybe emit Failure?
        }
    }

    public typealias ChangeSet = RealmCollectionChange<Self>
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Results: RealmSubscribable {
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension List: RealmSubscribable {
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension LinkingObjects: RealmSubscribable {
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmSubscription: Subscription {
    private var token: NotificationToken

    public var combineIdentifier: CombineIdentifier {
        return CombineIdentifier(token)
    }

    init(token: NotificationToken) {
        self.token = token
    }

    public func request(_ demand: Subscribers.Demand) {
    }

    public func cancel() {
        token.invalidate()
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmPublisher<Collection: RealmSubscribable>: Publisher where Collection: ThreadConfined {
    public typealias Failure = Never
    public typealias Output = Collection

    let object: Collection
    let freeze: Bool
    let queue: DispatchQueue?
    public init(_ object: Collection, freeze: Bool = false, queue: DispatchQueue? = nil) {
        self.object = object
        self.freeze = freeze
        self.queue = queue
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
        subscriber.receive(subscription: RealmSubscription(token: self.object.observe(on: queue, subscriber, freeze: freeze)))
    }

    public func subscribe<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil)
        -> RealmPublisher<Collection> where Collection: ThreadConfined {
            let queue = scheduler as! DispatchQueue
            return RealmPublisher(object, freeze: freeze, queue: queue)
    }

    public func receive<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil)
        -> RealmHandoverPublisher<Self, S> where Collection: ThreadConfined {
            return RealmHandoverPublisher(self, scheduler, self.object.realm!)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmHandoverPublisher<Upstream: Publisher, S: Scheduler>: Publisher where Upstream.Output: ThreadConfined {
    public typealias Failure = Upstream.Failure
    public typealias Output = Upstream.Output

    private let config: Realm.Configuration
    private let upstream: Upstream
    private let scheduler: S
    public init(_ upstream: Upstream, _ scheduler: S, _ realm: Realm) {
        self.config = realm.configuration
        self.upstream = upstream
        self.scheduler = scheduler
    }

    public func receive<Sub>(subscriber: Sub) where Sub: Subscriber, Sub.Failure == Failure, Output == Sub.Input {
        self.upstream
            .map { ThreadSafeReference(to: $0) }
            .receive(on: scheduler)
            .compactMap { $0.resolve(in: try! Realm(configuration: self.config,
                                                    queue: self.scheduler as? DispatchQueue)) }
            .receive(subscriber: subscriber)
    }
}

//@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
//public struct ThreadSafeRealmPublisher<Collection: RealmSubscribable>: Publisher where Collection: ThreadConfined {
//    public typealias Failure = Never
//    public typealias Output = Collection
//
//    let object: ThreadSafeReference<Collection>
//    let config: Realm.Configuration
//    let freeze: Bool
//    public init(_ object: Collection, freeze: Bool = false) {
//        self.object = ThreadSafeReference(to: object)
//        self.config = object.realm!.configuration
//        self.freeze = freeze
//    }
//
//    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
//        subscriber.receive(subscription: RealmSubscription(token: self.object.resolve(in: try! Realm(configuration: self.config))!.observe(subscriber, freeze: freeze)))
//    }
//}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmChangePublisher<Collection: RealmSubscribable>: Publisher {
    public typealias Output = (Collection, Collection.ChangeSet)
    public typealias Failure = Never

    let object: Collection
    let freeze: Bool
    let queue: DispatchQueue? = nil
    public init(_ object: Collection, freeze: Bool = false) {
        self.object = object
        self.freeze = freeze
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
        subscriber.receive(subscription: RealmSubscription(token: self.object.observe(on: queue, subscriber, freeze: freeze)))
    }
}

// MARK: - Results

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
//extension Results: Combine.ObservableObject, Identifiable {
extension Results {
    public var objectWillChange: RealmPublisher<Results> {
        RealmPublisher(self)
    }
}

// MARK: - ThreadSafeReference

/*
@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension ThreadSafeReference: RealmSubscribable where Confined: RealmSubscribable {
    public typealias ChangeSet = Confined.ChangeSet
    public typealias ObservedType = Confined

    public func observe<S>(_ subscriber: S, freeze: Bool) -> NotificationToken where Confined == S.Input, S : Subscriber {
        fatalError()
    }

    public func observe<S>(_ subscriber: S, freeze: Bool) -> NotificationToken where S : Subscriber, S.Input == (Confined, Confined.ChangeSet) {
        fatalError()
    }
}
 */

#endif // canImport(Combine)
